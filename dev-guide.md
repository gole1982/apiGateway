# API Gateway 开发规范

## 一、常见错误模式

### 1. 变量名与包名冲突

**问题**：使用与标准库包名相同的变量名

**错误示例**：
```go
import "log"

func main() {
    log := logger.NewLogger()  // 错误：与标准库包名冲突
}
```

**正确示例**：
```go
import "log"

func main() {
    logInstance := logger.NewLogger()  // 正确：使用不同名称
}
```

### 2. 未使用的导入

**问题**：导入了包但未使用

**错误示例**：
```go
import (
    "fmt"
    "log"  // 未使用
)
```

**正确示例**：移除未使用的导入

### 3. 未导出的字段/方法

**问题**：需要外部访问的成员未大写

**错误示例**：
```go
type Logger struct {
    storage *LogStorage  // 错误：外部无法访问
}
```

**正确示例**：
```go
type Logger struct {
    Storage *LogStorage  // 正确：首字母大写导出
}
```

### 4. 反引号字符串嵌套

**问题**：Go反引号字符串中包含JavaScript反引号

**错误示例**：
```go
html := `<div>${value}</div>`  // 错误：JS模板字符串与Go反引号冲突
```

**正确示例**：
```go
html := "<div>" + value + "</div>"  // 正确：使用字符串拼接
```

### 5. SQL结果处理

**问题**：忽略 db.Exec 返回值

**错误示例**：
```go
db.Exec("INSERT INTO ...")  // 错误：忽略返回值
```

**正确示例**：
```go
result, err := db.Exec("INSERT INTO ...")
if err != nil {
    return err
}
```

### 6. 字段名不一致

**问题**：前后端字段名不匹配

**错误示例**：
```go
// Go端
type Session struct {
    StartedAt time.Time `json:"started_at"`
}

// 前端期望
{ "start_time": "..." }
```

**正确示例**：
```go
type Session struct {
    StartedAt time.Time `json:"start_time"`  // 与前端一致
}
```

## 二、编码规范

### 1. 命名规范

| 类型 | 规则 | 示例 |
|------|------|------|
| 包名 | 小写，无下划线 | `logger`, `gateway` |
| 结构体 | 大驼峰 | `Logger`, `SessionTracker` |
| 接口 | 大驼峰，以er结尾 | `Storage`, `Reader` |
| 方法 | 大驼峰（导出）/小驼峰（私有） | `NewLogger()`, `generateID()` |
| 变量 | 小驼峰 | `requestID`, `logInstance` |
| 常量 | 全大写，下划线分隔 | `MAX_QUEUE_SIZE` |

### 2. 错误处理

- 始终检查并处理错误
- 使用 `errors.Is()` 和 `errors.As()` 进行错误判断
- 错误信息应清晰描述问题

### 3. 日志规范

- 使用标准库 `log` 包或统一的日志模块
- 避免使用 `fmt.Println` 和 `panic`

### 4. 注释规范

- 为导出的类型、函数、方法添加注释
- 注释应清晰说明功能和使用方式

## 三、Linter 使用

### 安装

```bash
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
```

### 运行

```bash
# 检查所有文件
golangci-lint run

# 检查指定文件
golangci-lint run ./internal/gateway/...

# 自动修复
golangci-lint run --fix
```

### 集成到IDE

1. 安装 golangci-lint 插件
2. 配置自动检测

## 四、代码审查检查清单

- [ ] 变量命名是否符合规范
- [ ] 是否有未使用的导入
- [ ] 错误是否正确处理
- [ ] 导出的成员是否正确大写
- [ ] SQL操作是否检查错误
- [ ] 前后端字段名是否一致
- [ ] 是否使用了禁止的函数（log变量、fmt.Println等）
- [ ] linter是否通过

## 五、提交规范

每次提交前应执行：

```bash
go build ./...          # 确保编译通过
go vet ./...            # 静态分析
golangci-lint run