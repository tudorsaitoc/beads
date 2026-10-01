//go:build cgo

package embeddeddolt_test

import (
	"context"
	"errors"
	"testing"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

func TestEmbeddedTransactionUpdateWithEventsCommitsOrRollsBackHistory(t *testing.T) {
	skipUnlessEmbeddedDolt(t)
	te := newTestEnv(t, "txev")
	ctx := t.Context()
	type eventWriter interface {
		UpdateIssueWithEvents(context.Context, string, map[string]interface{}, string) error
	}
	for _, ephemeral := range []bool{false, true} {
		name, id := "durable", "txev-perm"
		if ephemeral {
			name, id = "wisp", "txev-wisp"
		}
		t.Run(name, func(t *testing.T) {
			issue := &types.Issue{ID: id, Title: "before", Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask, Ephemeral: ephemeral}
			if err := te.store.CreateIssue(ctx, issue, "creator"); err != nil {
				t.Fatal(err)
			}
			aborted := errors.New("reject transaction")
			err := te.store.RunInTransaction(ctx, "aborted eventful update", func(tx storage.Transaction) error {
				writer, ok := tx.(eventWriter)
				if !ok {
					t.Fatal("embedded transaction lacks eventful update capability")
				}
				if err := writer.UpdateIssueWithEvents(ctx, id, map[string]interface{}{"status": "in_progress", "assignee": "new-owner"}, "aborted-actor"); err != nil {
					return err
				}
				return aborted
			})
			if !errors.Is(err, aborted) {
				t.Fatalf("rollback error = %v", err)
			}
			got, err := te.store.GetIssue(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != types.StatusOpen || got.Assignee != "" {
				t.Fatalf("aborted state persisted: %+v", got)
			}
			events, err := te.store.GetEvents(ctx, id, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range events {
				if event.Actor == "aborted-actor" {
					t.Fatalf("aborted event persisted: %+v", event)
				}
			}
			if err := te.store.RunInTransaction(ctx, "committed eventful update", func(tx storage.Transaction) error {
				return tx.(eventWriter).UpdateIssueWithEvents(ctx, id, map[string]interface{}{"status": "in_progress", "assignee": "new-owner"}, "winning-actor")
			}); err != nil {
				t.Fatal(err)
			}
			got, err = te.store.GetIssue(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != types.StatusInProgress || got.Assignee != "new-owner" {
				t.Fatalf("committed state lost: %+v", got)
			}
			events, err = te.store.GetEvents(ctx, id, 0)
			if err != nil {
				t.Fatal(err)
			}
			winning := 0
			for _, event := range events {
				if event.Actor == "winning-actor" && event.EventType == types.EventStatusChanged {
					winning++
				}
			}
			if winning != 1 {
				t.Fatalf("canonical status events for winning actor = %d, want one", winning)
			}
		})
	}
}
