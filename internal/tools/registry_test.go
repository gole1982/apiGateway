package tools

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sampleRegistry builds a Registry with a couple of preloaded tools for method
// tests. It deliberately avoids GetRegistry()/Reload() so it does not depend on
// the package-level db singleton (which cannot be injected from this package).
func sampleRegistry() *Registry {
	return &Registry{
		cache: map[string]*Tool{
			"web_search": {
				ID:           1,
				Name:         "web_search",
				Description:  "Search the web",
				Parameters:   `{"type":"object","properties":{"query":{"type":"string"}}}`,
				ExecutorType: "http",
				Enabled:      true,
				TimeoutMs:    5000,
			},
			"calc": {
				ID:           2,
				Name:         "calc",
				Description:  "Add two numbers",
				Parameters:   `{}`,
				ExecutorType: "builtin",
				Enabled:      false,
				TimeoutMs:    1000,
			},
			"empty_params": {
				ID:           3,
				Name:         "empty_params",
				Description:  "No params",
				Parameters:   "",
				ExecutorType: "http",
				Enabled:      true,
				TimeoutMs:    1000,
			},
		},
	}
}

func TestRegistry_Get(t *testing.T) {
	r := sampleRegistry()
	t.Run("found", func(t *testing.T) {
		got, err := r.Get("web_search")
		require.NoError(t, err)
		assert.Equal(t, "web_search", got.Name)
		assert.True(t, got.Enabled)
	})
	t.Run("not found", func(t *testing.T) {
		_, err := r.Get("nope")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
}

func TestRegistry_ListEnabled(t *testing.T) {
	r := sampleRegistry()
	list := r.ListEnabled()
	assert.Len(t, list, 2) // web_search + empty_params; calc is disabled
	names := map[string]bool{}
	for _, t := range list {
		names[t.Name] = true
	}
	assert.True(t, names["web_search"])
	assert.True(t, names["empty_params"])
	assert.False(t, names["calc"])
}

func TestRegistry_ListEnabled_Empty(t *testing.T) {
	r := &Registry{cache: map[string]*Tool{}}
	assert.Empty(t, r.ListEnabled())
}

func TestRegistry_ToOpenAITools_AllEnabled(t *testing.T) {
	r := sampleRegistry()
	toolsList, err := r.ToOpenAITools(nil) // empty names -> all enabled
	require.NoError(t, err)
	assert.Len(t, toolsList, 2)
}

func TestRegistry_ToOpenAITools_ByNames(t *testing.T) {
	r := sampleRegistry()
	toolsList, err := r.ToOpenAITools([]string{"web_search"})
	require.NoError(t, err)
	require.Len(t, toolsList, 1)
	entry := toolsList[0]
	assert.Equal(t, "function", entry["type"])
	fn := entry["function"].(map[string]interface{})
	assert.Equal(t, "web_search", fn["name"])
	assert.Equal(t, "Search the web", fn["description"])
	assert.NotNil(t, fn["parameters"])
}

func TestRegistry_ToOpenAITools_DisabledName(t *testing.T) {
	r := sampleRegistry()
	_, err := r.ToOpenAITools([]string{"calc"}) // calc is disabled
	require.Error(t, err)
	assert.Contains(t, err.Error(), "disabled")
}

func TestRegistry_ToOpenAITools_UnknownName(t *testing.T) {
	r := sampleRegistry()
	_, err := r.ToOpenAITools([]string{"unknown"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestToolToOpenAI_Structure(t *testing.T) {
	tool := &Tool{
		Name:        "t",
		Description: "d",
		Parameters:  `{"type":"object","properties":{"x":{"type":"string"}}}`,
	}
	entry, err := toolToOpenAI(tool)
	require.NoError(t, err)
	assert.Equal(t, "function", entry["type"])
	fn := entry["function"].(map[string]interface{})
	assert.Equal(t, "t", fn["name"])
	assert.Equal(t, "d", fn["description"])

	var params map[string]interface{}
	marshalled, err := json.Marshal(fn["parameters"])
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(marshalled, &params))
	assert.Equal(t, "object", params["type"])
}

func TestToolToOpenAI_DefaultParamsWhenEmpty(t *testing.T) {
	t.Run("empty string", func(t *testing.T) {
		entry, err := toolToOpenAI(&Tool{Name: "t", Parameters: ""})
		require.NoError(t, err)
		fn := entry["function"].(map[string]interface{})
		params, ok := fn["parameters"].(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, "object", params["type"])
		assert.NotNil(t, params["properties"])
	})
	t.Run("empty object {}", func(t *testing.T) {
		entry, err := toolToOpenAI(&Tool{Name: "t", Parameters: "{}"})
		require.NoError(t, err)
		fn := entry["function"].(map[string]interface{})
		params, ok := fn["parameters"].(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, "object", params["type"])
	})
}

func TestToolToOpenAI_InvalidParamsJSON(t *testing.T) {
	_, err := toolToOpenAI(&Tool{Name: "t", Parameters: `not json`})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid parameters JSON")
}
