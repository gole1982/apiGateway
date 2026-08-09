package apiformat

import "testing"

func TestIsGoogleNativeBaseURL(t *testing.T) {
	cases := []struct {
		base string
		want bool
	}{
		{"https://generativelanguage.googleapis.com", true},
		{"https://generativelanguage.googleapis.com/v1beta", true},
		{"https://us-central1-aiplatform.googleapis.com", true}, // Vertex
		{"https://api.openai.com", false},
		{"https://my-relay.example.com", false},
		{"", false},
		{"not a url", false},
	}
	for _, c := range cases {
		if got := IsGoogleNativeBaseURL(c.base); got != c.want {
			t.Errorf("IsGoogleNativeBaseURL(%q) = %v, want %v", c.base, got, c.want)
		}
	}
}

func TestBuildGoogleListModelsURL(t *testing.T) {
	got := BuildGoogleListModelsURL("https://generativelanguage.googleapis.com/v1beta", "my key?=x")
	want := "https://generativelanguage.googleapis.com/v1beta/models?key=my+key%3F%3Dx"
	if got != want {
		t.Errorf("BuildGoogleListModelsURL = %q, want %q", got, want)
	}

	// A base URL with extra path suffix should be normalised back to /v1beta/models.
	got = BuildGoogleListModelsURL("https://generativelanguage.googleapis.com/v1/chat/completions", "k")
	want = "https://generativelanguage.googleapis.com/v1beta/models?key=k"
	if got != want {
		t.Errorf("BuildGoogleListModelsURL (suffix) = %q, want %q", got, want)
	}
}

func TestParseGoogleListModelsResponse(t *testing.T) {
	raw := []byte(`{
		"models": [
			{"name": "models/gemini-2.0-flash", "supportedGenerationMethods": ["generateContent","streamGenerateContent"]},
			{"name": "models/text-embedding-004", "supportedGenerationMethods": ["embedContent"]},
			{"name": "models/gemini-1.5-pro"}
		]
	}`)
	got := ParseGoogleListModelsResponse(raw)
	// Embedding-only model filtered out; the pro model kept (no methods listed).
	want := []string{"gemini-2.0-flash", "gemini-1.5-pro"}
	if len(got) != len(want) {
		t.Fatalf("ParseGoogleListModelsResponse = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("ParseGoogleListModelsResponse[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	if ParseGoogleListModelsResponse([]byte("not json")) != nil {
		t.Errorf("expected nil for invalid JSON")
	}
}
