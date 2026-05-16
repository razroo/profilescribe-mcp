package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

func prepareActionProofPayload(ctx context.Context, cfg Config, payload []byte) ([]byte, error) {
	command := parseCommand(cfg.ActionProofCommand)
	if len(command) == 0 {
		return payload, nil
	}

	var req rpcRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, err
	}
	if req.Method != "tools/call" || len(req.Params) == 0 {
		return payload, nil
	}

	var params toolCallParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return nil, err
	}
	if params.Name != "create_timeline_draft" || len(params.Arguments) == 0 {
		return payload, nil
	}

	arguments := map[string]any{}
	if err := json.Unmarshal(params.Arguments, &arguments); err != nil {
		return nil, err
	}
	if hasActionProof(arguments) {
		return payload, nil
	}

	actionProof, err := runActionProofCommand(ctx, cfg, command, arguments)
	if err != nil {
		return nil, err
	}
	arguments["actionProof"] = actionProof

	nextArguments, err := json.Marshal(arguments)
	if err != nil {
		return nil, err
	}
	params.Arguments = nextArguments

	nextParams, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	req.Params = nextParams
	return json.Marshal(req)
}

func runActionProofCommand(ctx context.Context, cfg Config, command []string, arguments map[string]any) (json.RawMessage, error) {
	input := map[string]any{
		"tool": map[string]any{
			"name":      "create_timeline_draft",
			"arguments": arguments,
		},
		"draft":  arguments,
		"mcpUrl": cfg.MCPURL,
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, command[0], command[1:]...)
	cmd.Stdin = bytes.NewReader(payload)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, errors.New("ActionProof producer command timed out")
		}
		return nil, fmt.Errorf("ActionProof producer command failed: %s", sanitizeCommandError(stderr.String(), err))
	}

	actionProof, err := extractActionProof(stdout.Bytes())
	if err != nil {
		return nil, err
	}
	return actionProof, nil
}

func extractActionProof(stdout []byte) (json.RawMessage, error) {
	trimmed := bytes.TrimSpace(stdout)
	if len(trimmed) == 0 {
		return nil, errors.New("ActionProof producer command returned empty output")
	}

	var wrapped struct {
		ActionProof json.RawMessage `json:"actionProof"`
	}
	if err := json.Unmarshal(trimmed, &wrapped); err == nil && len(bytes.TrimSpace(wrapped.ActionProof)) > 0 {
		if !isJSONObject(wrapped.ActionProof) {
			return nil, errors.New("ActionProof producer command returned non-object actionProof")
		}
		return wrapped.ActionProof, nil
	}
	if !isJSONObject(trimmed) {
		return nil, errors.New("ActionProof producer command output must be a JSON object")
	}
	return json.RawMessage(append([]byte(nil), trimmed...)), nil
}

func hasActionProof(arguments map[string]any) bool {
	value, ok := arguments["actionProof"]
	if !ok || value == nil {
		return false
	}
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text) != ""
	}
	if object, ok := value.(map[string]any); ok {
		return len(object) > 0
	}
	return true
}

func parseCommand(raw string) []string {
	fields := strings.Fields(strings.TrimSpace(raw))
	if len(fields) == 0 {
		return nil
	}
	return fields
}

func isJSONObject(raw []byte) bool {
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		return false
	}
	return object != nil
}

func sanitizeCommandError(stderr string, fallback error) string {
	message := strings.TrimSpace(stderr)
	if message == "" {
		message = fallback.Error()
	}
	message = strings.ReplaceAll(message, "\n", " ")
	if len(message) > 500 {
		message = message[:500]
	}
	return message
}
