package runner

import (
	"context"

	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-state/query"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/backend"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

// Exercise the configured serving path on the reopened database. The block
// execution root is the trust anchor; verification remains outside assembly.
// Account/storage requests use the same bounded sample as the checked reader.
func verifyConfiguredQueries(b *backend.Backend, height uint64, root mpt.Hash, addresses []common.Address, report *Report) error {
	ctx := context.Background()
	for _, address := range addresses {
		for slot := -1; slot < 4; slot++ {
			req := query.Request{Height: height, Kind: query.Account, Address: address}
			if slot >= 0 {
				req.Kind = query.Storage
				req.Slot[31] = byte(slot)
			}
			result, stats, err := b.QueryWithProofStats(ctx, req)
			if err != nil {
				return err
			}
			if _, err = query.VerifyWithCodec(ctx, root, req, result, query.DefaultLimits(), codec.Ethereum); err != nil {
				return err
			}
			report.QueryRequests++
			report.QueryWorkersStarted += stats.WorkerStarts
			report.QueryBackgroundReads += stats.BackgroundReads
			if slot < 0 {
				report.AccountProofs++
			} else {
				report.StorageProofs++
			}
		}
	}
	return nil
}
