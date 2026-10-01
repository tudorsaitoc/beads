package dolt

import (
	"context"
	"errors"
	"testing"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

func TestSingleTransactionMixedTierCommitRollbackAndSessionRestore(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx, cancel := testContext(t)
	defer cancel()
	ids := []string{"test-single-perm", "test-single-wisp"}
	for index, id := range ids {
		issue := &types.Issue{ID: id, Title: "before", Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask, Ephemeral: index == 1}
		if err := store.CreateIssue(ctx, issue, "creator"); err != nil {
			t.Fatal(err)
		}
	}
	var beforeMode int64
	if err := store.db.QueryRowContext(ctx, "SELECT @@SESSION.dolt_transaction_commit").Scan(&beforeMode); err != nil {
		t.Fatal(err)
	}
	type eventWriter interface {
		UpdateIssueWithEvents(context.Context, string, map[string]interface{}, string) error
	}
	rejected := errors.New("later subject guard rejected")
	mutate := func(tx storage.Transaction, actor string) error {
		writer, ok := tx.(eventWriter)
		if !ok {
			t.Fatal("single transaction lacks native event writer")
		}
		for _, id := range ids {
			if err := writer.UpdateIssueWithEvents(ctx, id, map[string]interface{}{"status": "in_progress", "assignee": "new-owner"}, actor); err != nil {
				return err
			}
		}
		return nil
	}
	err := store.RunInSingleTransaction(ctx, "aborted mixed activation", func(tx storage.Transaction) error {
		if err := mutate(tx, "aborted-actor"); err != nil {
			return err
		}
		return rejected
	})
	if !errors.Is(err, rejected) {
		t.Fatalf("mixed rollback error = %v", err)
	}
	for _, id := range ids {
		got, err := store.GetIssue(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != types.StatusOpen || got.Assignee != "" {
			t.Fatalf("aborted mixed state persisted: %+v", got)
		}
		events, err := store.GetEvents(ctx, id, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range events {
			if event.Actor == "aborted-actor" {
				t.Fatalf("aborted event persisted: %+v", event)
			}
		}
	}
	if err := store.RunInSingleTransaction(ctx, "committed mixed activation", func(tx storage.Transaction) error { return mutate(tx, "single-winner") }); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		got, err := store.GetIssue(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != types.StatusInProgress || got.Assignee != "new-owner" {
			t.Fatalf("single committed state missing: %+v", got)
		}
		events, err := store.GetEvents(ctx, id, 0)
		if err != nil {
			t.Fatal(err)
		}
		winners := 0
		for _, event := range events {
			if event.Actor == "single-winner" && event.EventType == types.EventStatusChanged {
				winners++
			}
		}
		if winners != 1 {
			t.Fatalf("canonical single-session winning events for %s = %d", id, winners)
		}
	}
	var afterMode int64
	if err := store.db.QueryRowContext(ctx, "SELECT @@SESSION.dolt_transaction_commit").Scan(&afterMode); err != nil {
		t.Fatal(err)
	}
	if afterMode != beforeMode {
		t.Fatalf("pooled session commit mode leaked: before %d after %d", beforeMode, afterMode)
	}
	var committed int
	if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events AS OF 'HEAD' WHERE issue_id = ? AND actor = ?", ids[0], "single-winner").Scan(&committed); err != nil {
		t.Fatal(err)
	}
	if committed != 1 {
		t.Fatalf("durable event not committed to canonical history: %d", committed)
	}
}

func TestSingleTransactionPostSQLHistoryFailureDoesNotReplay(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx, cancel := testContext(t)
	defer cancel()
	issue := &types.Issue{ID: "test-single-history", Title: "before", Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask}
	if err := store.CreateIssue(ctx, issue, "creator"); err != nil {
		t.Fatal(err)
	}
	callbacks := 0
	err := store.RunInSingleTransaction(ctx, "forced history failure", func(tx storage.Transaction) error {
		callbacks++
		if err := tx.UpdateIssue(ctx, issue.ID, map[string]interface{}{"assignee": "committed-owner"}, "history-actor"); err != nil {
			return err
		}
		tx.(*doltTransaction).dirty.MarkDirty("missing_control_history_table")
		return nil
	})
	var committed interface{ SQLCommitted() bool }
	if err == nil || !errors.As(err, &committed) || !committed.SQLCommitted() {
		t.Fatalf("history failure concealed committed SQL: %v", err)
	}
	if callbacks != 1 {
		t.Fatalf("post-SQL failure replayed callback %d times", callbacks)
	}
	got, getErr := store.GetIssue(ctx, issue.ID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if got.Assignee != "committed-owner" {
		t.Fatalf("history failure falsely represented rollback: %+v", got)
	}
}
