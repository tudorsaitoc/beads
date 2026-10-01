package dolt

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
)

func TestNormalWritersPreserveTerminalClockAndLabelFence(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx, cancel := testContext(t)
	defer cancel()
	const first = "2026-10-01T01:00:00+00:00"
	for _, ephemeral := range []bool{false, true} {
		table, id := "issues", "test-clock-perm"
		if ephemeral {
			table, id = "wisps", "test-clock-wisp"
		}
		t.Run(table, func(t *testing.T) {
			issue := &types.Issue{ID: id, Title: "before", Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask, Ephemeral: ephemeral, Metadata: json.RawMessage(`{"retained":"keep"}`)}
			if err := store.CreateIssue(ctx, issue, "creator"); err != nil {
				t.Fatal(err)
			}
			if err := store.UpdateIssue(ctx, id, map[string]interface{}{issueops.OpSetMetadata: []string{"refinery_decision_at=" + first, "outcome=merged"}}, "first-clock"); err != nil {
				t.Fatal(err)
			}
			for _, updates := range []map[string]interface{}{
				{issueops.OpSetMetadata: []string{"refinery_decision_at=2026-10-01T02:00:00Z", "ordinary=changed"}},
				{issueops.OpUnsetMetadata: []string{"refinery_decision_at", "outcome"}},
				{issueops.OpMergeMetadata: json.RawMessage(`{"refinery_decision_at":"","merged":"yes"}`)},
				{"metadata": json.RawMessage(`{"replacement":"yes"}`)},
			} {
				if err := store.UpdateIssue(ctx, id, updates, "ordinary-writer"); err != nil {
					t.Fatal(err)
				}
				got, err := store.GetIssue(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				var metadata map[string]string
				if err := json.Unmarshal(got.Metadata, &metadata); err != nil {
					t.Fatal(err)
				}
				if metadata["refinery_decision_at"] != first {
					t.Fatalf("ordinary writer changed first clock: %s", got.Metadata)
				}
			}
			got, err := store.GetIssue(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			imported := *got
			imported.Title = "imported"
			imported.Metadata = json.RawMessage(`{"from_import":"yes"}`)
			if err := store.RunInTransaction(ctx, "clock-safe import", func(tx storage.Transaction) error {
				native := tx.(*doltTransaction)
				if err := issueops.InsertIssueIntoTable(ctx, native.txFor(table), table, &imported); err != nil {
					return err
				}
				native.dirty.MarkDirty(table)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			got, err = store.GetIssue(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			var importedMetadata map[string]string
			if err := json.Unmarshal(got.Metadata, &importedMetadata); err != nil {
				t.Fatal(err)
			}
			if got.Title != "imported" || importedMetadata["refinery_decision_at"] != first || importedMetadata["from_import"] != "yes" {
				t.Fatalf("upsert lost clock or requested import: %+v", got)
			}
			readFence := func() string {
				var token string
				if err := store.RunInTransaction(ctx, "read label collision cell", func(tx storage.Transaction) error {
					native := tx.(*doltTransaction)
					return native.txFor(table).QueryRowContext(ctx, fmt.Sprintf("SELECT CAST(row_lock AS CHAR) FROM %s WHERE id = ?", table), id).Scan(&token)
				}); err != nil {
					t.Fatal(err)
				}
				return token
			}
			mutations := []struct {
				name       string
				write      func() error
				wantLabels int
			}{
				{"standalone-add", func() error { return store.AddLabel(ctx, id, "hold", "label-writer") }, 1},
				{"standalone-remove", func() error { return store.RemoveLabel(ctx, id, "hold", "label-writer") }, 0},
				{"transaction-add", func() error {
					return store.RunInTransaction(ctx, "label add", func(tx storage.Transaction) error { return tx.AddLabel(ctx, id, "hold", "label-writer") })
				}, 1},
				{"transaction-remove", func() error {
					return store.RunInTransaction(ctx, "label remove", func(tx storage.Transaction) error { return tx.RemoveLabel(ctx, id, "hold", "label-writer") })
				}, 0},
			}
			for _, mutation := range mutations {
				before := readFence()
				if err := mutation.write(); err != nil {
					t.Fatalf("%s: %v", mutation.name, err)
				}
				after := readFence()
				labels, err := store.GetLabels(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				if len(labels) != mutation.wantLabels || (len(labels) == 1 && labels[0] != "hold") {
					t.Fatalf("%s label state = %v", mutation.name, labels)
				}
				if before == after {
					t.Fatalf("%s silently bypassed the issue's collision fence", mutation.name)
				}
				if !ephemeral {
					var committed string
					if err := store.db.QueryRowContext(ctx, "SELECT CAST(row_lock AS CHAR) FROM issues AS OF 'HEAD' WHERE id = ?", id).Scan(&committed); err != nil {
						t.Fatal(err)
					}
					if committed != after {
						t.Fatalf("%s failed to stage the issue fence with its label mutation", mutation.name)
					}
				}
			}
		})
	}
}

func TestInvalidFirstTerminalClockRollsBackWholeUpdate(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx, cancel := testContext(t)
	defer cancel()
	for index, clock := range []interface{}{"not-a-clock", "2026-10-01T01:00:00+01:00", 42} {
		id := fmt.Sprintf("test-clock-invalid-%d", index)
		issue := &types.Issue{ID: id, Title: "before", Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask, Metadata: json.RawMessage(`{"retained":"keep"}`)}
		if err := store.CreateIssue(ctx, issue, "creator"); err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(map[string]interface{}{"refinery_decision_at": clock, "outcome": "must-not-write"})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.UpdateIssue(ctx, id, map[string]interface{}{"metadata": json.RawMessage(raw), "status": "closed", "assignee": "must-not-transfer"}, "invalid-clock"); err == nil {
			t.Fatalf("invalid clock %v accepted", clock)
		}
		got, err := store.GetIssue(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != types.StatusOpen || got.Assignee != "" || got.ClosedAt != nil || string(got.Metadata) != string(issue.Metadata) {
			t.Fatalf("invalid clock partially committed state: %+v", got)
		}
	}
}

func TestLabelOnlyChangesKeepTimestampTieImportEligible(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx, cancel := testContext(t)
	defer cancel()
	for _, ephemeral := range []bool{false, true} {
		table, id := "issues", "test-import-label-perm"
		if ephemeral {
			table, id = "wisps", "test-import-label-wisp"
		}
		t.Run(table, func(t *testing.T) {
			at := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
			issue := &types.Issue{ID: id, Title: "original goal", Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask, Ephemeral: ephemeral, CreatedAt: at, UpdatedAt: at}
			if err := store.CreateIssue(ctx, issue, "creator"); err != nil {
				t.Fatal(err)
			}
			snapshot, err := store.GetIssue(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.AddLabel(ctx, id, "existing", "label-writer"); err != nil {
				t.Fatal(err)
			}
			labeled, err := store.GetIssue(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if !labeled.UpdatedAt.Equal(snapshot.UpdatedAt) {
				t.Fatalf("label-only mutation made a same-content import stale: before=%s after=%s", snapshot.UpdatedAt, labeled.UpdatedAt)
			}
			snapshot.Title = "timestamp tie must not rewrite the goal"
			snapshot.Labels = []string{"imported-hold"}
			if err := store.RunInTransaction(ctx, "timestamp-tie label import", func(tx storage.Transaction) error {
				native := tx.(*doltTransaction)
				bc, err := issueops.NewBatchContext(ctx, native.txFor(table), storage.BatchCreateOptions{RejectStaleUpserts: true})
				if err != nil {
					return err
				}
				result, err := issueops.CreateIssueInTxWithResult(ctx, native.txFor(table), bc, snapshot, "importer")
				if err != nil {
					return err
				}
				if result.StaleRejected {
					return fmt.Errorf("label-only change rejected timestamp-tie import")
				}
				for changed := range result.ChangedTables {
					native.dirty.MarkDirty(changed)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			got, err := store.GetIssue(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if got.Title != "original goal" || !got.UpdatedAt.Equal(labeled.UpdatedAt) {
				t.Fatalf("timestamp-tie label import rewrote issue content: %+v", got)
			}
			labels, err := store.GetLabels(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if len(labels) != 2 || labels[0] != "existing" || labels[1] != "imported-hold" {
				t.Fatalf("timestamp-tie import lost additive hold: %v", labels)
			}
		})
	}
}
