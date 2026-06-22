package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type BootstrapResponse struct {
	JWT string `json:"jwt"`
	Exp int64  `json:"exp"`
}

func main() {
	clientID := "mimo-jwt-tool-" + fmt.Sprintf("%d", time.Now().UnixNano())
	if len(os.Args) > 1 {
		clientID = os.Args[1]
	}

	reqBody := map[string]string{"client": clientID}
	jsonBody, _ := json.Marshal(reqBody)

	resp, err := http.Post(
		"https://api.xiaomimimo.com/api/free-ai/bootstrap",
		"application/json",
		bytes.NewBuffer(jsonBody),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "HTTP %d: %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}

	var result BootstrapResponse
	if err := json.Unmarshal(body, &result); err != nil {
		fmt.Fprintf(os.Stderr, "Parse error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(result.JWT)
}