// 本文件手写 Prometheus 文本曝光格式（text exposition format v0.0.4），
// 不引入 prometheus/client_golang。
//
// 为什么手写：本项目是单二进制 + 无 CGO 的自部署工具，依赖树刻意保持极小
// （go.mod 只有 4 个直接依赖）。client_golang 会带进 ~10 个传递依赖，而这里
// 需要的只是十几行字符串拼接。手写换来的是零依赖代价，代价是必须自己保证
// 格式合法 —— 故下面每种样本类型都有对应的单测。
//
// 暴露的指标分两类：
//   - 进程/运行时瞬时量（gauge）：从 scheduler 快照与 DB 读，语义是"此刻"。
//   - 累计量（counter）：直接来自 request_logs 的 COUNT/SUM，本身单调递增，
//     重启后不会归零（DB 持久），所以可以安全地当 counter 用。
package prom

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Registry 收集一次曝光所需的全部样本。
//
// 刻意不抄 Prometheus 的 Collector 接口：本项目只有一个使用者（/metrics
// handler），且指标集合是静态的，抽象一层只会增加间接。
type Registry struct {
	gauges   []sample
	counters []sample
}

type sample struct {
	name   string
	labels []labelPair
	value  float64
}

type labelPair struct {
	k string
	v string
}

// New 构造一个空 registry。
func New() *Registry {
	return &Registry{}
}

// Gauge 记录一个瞬时量。
//
// name 必须匹配 [a-zA-Z_:][a-zA-Z0-9_:]*，否则返回错误而不是静默产出
// 无法被 Prometheus 解析的一行 —— 宁可让 /metrics 报错也不要写出坏格式。
func (r *Registry) Gauge(name string, value float64, labelPairs ...string) error {
	return r.add(&r.gauges, name, value, labelPairs)
}

// Counter 记录一个累计量。
func (r *Registry) Counter(name string, value float64, labelPairs ...string) error {
	return r.add(&r.counters, name, value, labelPairs)
}

func (r *Registry) add(dst *[]sample, name string, value float64, labelPairs []string) error {
	if !validMetricName(name) {
		return fmt.Errorf("prom: invalid metric name %q", name)
	}
	labels, err := parseLabels(labelPairs)
	if err != nil {
		return err
	}
	*dst = append(*dst, sample{name: name, labels: labels, value: value})
	return nil
}

// validMetricName 校验 Prometheus 指标名：首字符为字母/下划线/冒号，
// 其余可含数字。刻意不接受 '-'（那是保留给 recording rule 的）。
func validMetricName(name string) bool {
	if name == "" {
		return false
	}
	for i, c := range name {
		isAlpha := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		if i == 0 {
			if !isAlpha && c != '_' && c != ':' {
				return false
			}
			continue
		}
		if !isAlpha && !(c >= '0' && c <= '9') && c != '_' && c != ':' {
			return false
		}
	}
	return true
}

// parseLabels 把 k1,v1,k2,v2 扁平列表转成有序 label 对。
//
// 要求偶数长度且 key 合法/唯一：Prometheus 对重复 label key 是拒绝解析的，
// 在这里就报错比在 Prometheus 侧看到一坨看不懂的解析错误要好定位。
func parseLabels(flat []string) ([]labelPair, error) {
	if len(flat)%2 != 0 {
		return nil, fmt.Errorf("prom: labels must be k,v pairs, got %d elements", len(flat))
	}
	out := make([]labelPair, 0, len(flat)/2)
	seen := make(map[string]bool, len(flat)/2)
	for i := 0; i < len(flat); i += 2 {
		k, v := flat[i], flat[i+1]
		if !validLabelName(k) {
			return nil, fmt.Errorf("prom: invalid label name %q", k)
		}
		if seen[k] {
			return nil, fmt.Errorf("prom: duplicate label %q", k)
		}
		seen[k] = true
		out = append(out, labelPair{k: k, v: v})
	}
	// 排序让输出稳定：map 遍历顺序随机会让 /metrics 的文本每次抓取都不同，
	// 徒增 diff 与告警噪声。
	sort.Slice(out, func(a, b int) bool { return out[a].k < out[b].k })
	return out, nil
}

func validLabelName(k string) bool {
	if k == "" {
		return false
	}
	for i, c := range k {
		isAlpha := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		if i == 0 {
			if !isAlpha && c != '_' {
				return false
			}
			continue
		}
		if !isAlpha && !(c >= '0' && c <= '9') && c != '_' {
			return false
		}
	}
	return true
}

// Gather 渲染成 Prometheus 文本曝光格式。
//
// 排序规则：先按指标名，名字相同再按 label 串。理由同 parseLabels ——
// 输出必须可复现，否则每次抓取都是不同的字节，Prometheus 侧的
// change detection 会持续报"指标变了"。
func (r *Registry) Gather() string {
	var b strings.Builder

	writeSamples := func(samples []sample, typ string) {
		if len(samples) == 0 {
			return
		}
		sorted := make([]sample, len(samples))
		copy(sorted, samples)
		sort.Slice(sorted, func(a, b int) bool {
			if sorted[a].name != sorted[b].name {
				return sorted[a].name < sorted[b].name
			}
			return labelKey(sorted[a].labels) < labelKey(sorted[b].labels)
		})
		lastName := ""
		for _, s := range sorted {
			if s.name != lastName {
				fmt.Fprintf(&b, "# TYPE %s %s\n", s.name, typ)
				lastName = s.name
			}
			b.WriteString(s.name)
			if len(s.labels) > 0 {
				b.WriteString("{")
				for i, lp := range s.labels {
					if i > 0 {
						b.WriteString(",")
					}
					b.WriteString(lp.k)
					b.WriteString(`="`)
					b.WriteString(escapeLabelValue(lp.v))
					b.WriteString(`"`)
				}
				b.WriteString("}")
			}
			b.WriteString(" ")
			b.WriteString(formatValue(s.value))
			b.WriteString("\n")
		}
	}

	writeSamples(r.counters, "counter")
	writeSamples(r.gauges, "gauge")
	return b.String()
}

func labelKey(labels []labelPair) string {
	var b strings.Builder
	for _, lp := range labels {
		b.WriteString(lp.k)
		b.WriteString("=")
		b.WriteString(lp.v)
		b.WriteString(";")
	}
	return b.String()
}

// escapeLabelValue 转义 label 值里三个有语法意义的字符。反斜杠必须第一个
// 处理，否则后面补上的转义符本身会被再转义一遍。
func escapeLabelValue(v string) string {
	r := strings.NewReplacer(
		`\`, `\\`,
		`"`, `\"`,
		"\n", `\n`,
	)
	return r.Replace(v)
}

// formatValue 用紧凑且不失真的方式渲染数值。
//
// 整数走 %d 输出 "0" 而不是 "0.000000"：Prometheus 两种都接受，但前者
// 让 /metrics 的文本可读得多，也更容易被人眼 diff。
func formatValue(v float64) string {
	if v == float64(int64(v)) {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}
