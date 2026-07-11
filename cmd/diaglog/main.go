package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"

	_ "modernc.org/sqlite"
)

func main() {
	db, err := sql.Open("sqlite", "gateway.db")
	if err != nil {
		fmt.Println("open:", err)
		os.Exit(1)
	}
	defer db.Close()

	// Find the last request that contains "hello"
	rows, err := db.Query(`
		SELECT e.request_id, e.event_type, e.timestamp, e.data
		FROM log_events e
		WHERE e.request_id IN (
			SELECT request_id FROM log_events
			WHERE event_type='REQUEST_RECEIVED' AND data LIKE '%"hello"%'
			ORDER BY timestamp DESC LIMIT 3
		)
		ORDER BY e.request_id, e.timestamp
	`)
	if err != nil {
		fmt.Println("query:", err)
		os.Exit(1)
	}
	defer rows.Close()

	prevReq := ""
	for rows.Next() {
		var reqID, evtType, ts, dataStr string
		rows.Scan(&reqID, &evtType, &ts, &dataStr)

		if reqID != prevReq {
			fmt.Printf("\n========== request_id=%s ==========\n", reqID)
			prevReq = reqID
		}

		var data map[string]interface{}
		json.Unmarshal([]byte(dataStr), &data)

		fmt.Printf("  [%s] %s\n", evtType, ts)

		switch evtType {
		case "REQUEST_RECEIVED":
			fmt.Printf("    path=%v\n", data["request_path"])
			body, _ := data["request_body"].(string)
			// find the user content "hello"
			var rb map[string]interface{}
			if json.Unmarshal([]byte(body), &rb) == nil {
				fmt.Printf("    model=%v  stream=%v\n", rb["model"], rb["stream"])
				if msgs, ok := rb["messages"].([]interface{}); ok {
					for _, m := range msgs {
						msg, _ := m.(map[string]interface{})
						if c, _ := msg["content"].(string); c == "hello" {
							fmt.Printf("    -> user content: %q\n", c)
						}
					}
				}
			}
		case "UPSTREAM_SENT":
			fmt.Printf("    rapi=%v  url=%v  retry=%v\n", data["selected_rapi"], data["upstream_url"], data["retry_count"])
			body, _ := data["upstream_body"].(string)
			var ub map[string]interface{}
			if json.Unmarshal([]byte(body), &ub) == nil {
				fmt.Printf("    upstream_model=%v  stream=%v\n", ub["model"], ub["stream"])
			}
		case "UPSTREAM_RESPONSE":
			fmt.Printf("    status=%v  latency=%vms  finish=%v\n", data["response_status"], data["latency_ms"], data["finish_reason"])
			resp, _ := data["response_body"].(string)
			// show last 300 chars of SSE body
			if len(resp) > 300 {
				resp = "..." + resp[len(resp)-300:]
			}
			fmt.Printf("    body_tail: %s\n", resp)
		case "CLIENT_RESPONSE":
			fmt.Printf("    status=%v  latency=%vms  fallback=%v\n", data["response_status"], data["latency_ms"], data["fallback_used"])
		case "ERROR":
			fmt.Printf("    stage=%v  msg=%v\n", data["stage"], data["error_message"])
		}
	}
}
