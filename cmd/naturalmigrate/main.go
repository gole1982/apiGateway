// naturalmigrate 是自然键身份迁移（credential + endpoint_credential）的命令行工具。
//
// 默认 dry-run：只读分析旧库（platform_keys / rapi.alias 身份体系）将发生的
// 合并，打印清单但不落任何写；确认后加 -apply 才真正执行。迁移幂等，且与
// db.Init 启动时自动执行的是同一条代码路径 —— 正常升级无需手动跑，本工具
// 仅用于升级前审阅合并清单（credential 按 token_hash、端点按 (platform, model)、
// 平台按归一化 base_url）。
//
// 用法：
//
//	go run ./cmd/naturalmigrate -db gateway.db           # dry-run 预览
//	go run ./cmd/naturalmigrate -db gateway.db -apply    # 真正迁移
//
// 设计：docs/superpowers/specs/2026-09-23-natural-key-identity-design.md §6。
package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"gateway/internal/crypto"
	"gateway/internal/db"

	_ "modernc.org/sqlite"
)

func main() {
	dbPath := flag.String("db", "gateway.db", "SQLite 数据库路径")
	apply := flag.Bool("apply", false, "真正执行迁移（默认仅 dry-run 预览，不落库）")
	flag.Parse()

	// token_hash 需要解开本地密文 token → 用本机 ~/.apiGateway.key。
	if err := crypto.Init(); err != nil {
		fmt.Fprintln(os.Stderr, "crypto init（迁移需解密 token 计算 hash）:", err)
		os.Exit(1)
	}

	conn, err := sql.Open("sqlite", *dbPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer conn.Close()

	var rep *db.NaturalKeyReport
	if *apply {
		rep, err = db.MigrateNaturalKeysConn(conn)
	} else {
		rep, err = db.PreviewNaturalKeyMigration(conn)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}

	out, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(out))
	if !*apply {
		fmt.Println("\n（dry-run：未写入任何变更；确认清单无误后加 -apply 执行）")
	}
}
