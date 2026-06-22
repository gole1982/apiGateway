package logger

import "net/http"

func SanitizeKey(key string) string {
	if len(key) <= 8 {
		if len(key) <= 4 {
			return key
		}
		return key[:2] + "****" + key[len(key)-2:]
	}
	return key[:4] + "****" + key[len(key)-4:]
}

func SanitizeHeaders(headers http.Header) http.Header {
	sanitized := make(http.Header)
	for k, v := range headers {
		if k == "Authorization" || k == "authorization" {
			sanitized[k] = []string{SanitizeKey(v[0])}
		} else {
			sanitized[k] = v
		}
	}
	return sanitized
}