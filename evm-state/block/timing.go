package block

import "time"

// ExecutionTiming marks exclusive boundaries in a live block commit. The caller
// times the whole adapter call, including deferred view releases. A nil observer
// keeps timers disabled on the production and logical-import paths.
type ExecutionTiming struct{ Transactions, Materialize, Publish time.Time }

func (t *ExecutionTiming) StartTransactions() {
	if t != nil {
		t.Transactions = time.Now()
	}
}
func (t *ExecutionTiming) StartMaterialize() {
	if t != nil {
		t.Materialize = time.Now()
	}
}
func (t *ExecutionTiming) StartPublish() {
	if t != nil {
		t.Publish = time.Now()
	}
}
