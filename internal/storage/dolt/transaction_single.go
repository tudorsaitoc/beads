package dolt

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/versioncontrolops"
)

// sqlCommittedError distinguishes a history/cleanup failure from a rolled-back
// SQL mutation. Consumers must reconcile committed state, never replay it.
type sqlCommittedError struct{ cause error }

func (e *sqlCommittedError) Error() string {
	return fmt.Sprintf("SQL already committed; finalization failed: %v", e.cause)
}
func (e *sqlCommittedError) Unwrap() error      { return e.cause }
func (e *sqlCommittedError) SQLCommitted() bool { return true }

// RunInSingleTransaction applies durable and ignored-table mutations through
// one SQL session and one SQL COMMIT. The legacy two-session API is unchanged.
// There is deliberately no callback replay after SQL commit or history failure.
func (s *DoltStore) RunInSingleTransaction(ctx context.Context, commitMsg string, fn func(storage.Transaction) error) (retErr error) {
	if fn == nil {
		return errors.New("single transaction: nil callback")
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire single transaction connection: %w", err)
	}
	var previous int64
	if err := conn.QueryRowContext(ctx, "SELECT @@SESSION.dolt_transaction_commit").Scan(&previous); err != nil {
		_ = conn.Close()
		return fmt.Errorf("read transaction commit mode: %w", err)
	}
	committed := false
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(cleanupCtx, "SET @@SESSION.dolt_transaction_commit = ?", previous); err != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			resetErr := fmt.Errorf("restore transaction commit mode: %w", err)
			if committed {
				resetErr = &sqlCommittedError{cause: resetErr}
			}
			retErr = errors.Join(retErr, resetErr)
		}
		_ = conn.Close()
	}()
	if _, err := conn.ExecContext(ctx, "SET @@SESSION.dolt_transaction_commit = 0"); err != nil {
		return fmt.Errorf("set single transaction commit mode: %w", err)
	}
	sqlTx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin single transaction: %w", err)
	}
	defer func() {
		if committed {
			return
		}
		if err := sqlTx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			retErr = errors.Join(retErr, fmt.Errorf("rollback single transaction: %w", err))
		}
	}()
	tx := &doltTransaction{regularTx: sqlTx, ignoredTx: sqlTx, store: s}
	if err := fn(tx); err != nil {
		return err
	}
	if err := sqlTx.Commit(); err != nil {
		return fmt.Errorf("single SQL commit: %w", err)
	}
	committed = true
	if err := versioncontrolops.StageAndCommit(ctx, conn, tx.dirty.DirtyTables(), commitMsg, s.commitAuthorString()); err != nil {
		return &sqlCommittedError{cause: err}
	}
	return nil
}
