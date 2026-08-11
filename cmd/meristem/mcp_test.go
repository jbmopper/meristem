package main

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestListenerTaskMCPBindingEnvironmentIsAllOrNothingAndCanonical(t *testing.T) {
	keys := []string{
		"MERISTEM_MCP_EXPECT_ACTOR_ID",
		"MERISTEM_MCP_LISTENER_ACTIVATION_ID",
		"MERISTEM_MCP_LISTENER_WORK_ITEM_ID",
		"MERISTEM_MCP_LISTENER_ASSIGNMENT_EVENT_ID",
	}
	for _, key := range keys {
		t.Setenv(key, "")
	}
	if got, err := listenerTaskMCPBindingFromEnv(); err != nil || got != nil {
		t.Fatalf("absent binding=%+v err=%v", got, err)
	}

	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	for i, key := range keys {
		t.Setenv(key, ids[i].String())
	}
	got, err := listenerTaskMCPBindingFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if got.ExpectedActorID != ids[0] || got.ActivationID != ids[1] || got.WorkItemID != ids[2] || got.AssignmentEventID != ids[3] {
		t.Fatalf("parsed task binding=%+v", got)
	}

	t.Setenv(keys[3], "")
	if _, err := listenerTaskMCPBindingFromEnv(); err == nil || !strings.Contains(err.Error(), "all present or all absent") {
		t.Fatalf("partial binding error=%v", err)
	}
	t.Setenv(keys[3], ids[3].String())
	for name, malformed := range map[string]string{
		"nil":        uuid.Nil.String(),
		"uppercase":  strings.ToUpper(ids[0].String()),
		"whitespace": " " + ids[0].String(),
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(keys[0], malformed)
			if _, err := listenerTaskMCPBindingFromEnv(); err == nil {
				t.Fatalf("malformed expected actor %q accepted", malformed)
			}
		})
	}
}
