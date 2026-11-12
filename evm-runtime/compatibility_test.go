package evmruntime

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"os"
	"strconv"
	"testing"

	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/core/types"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/rlp"
)

// Fixed vectors were produced with the unmodified upstream version. Tests use
// only local code and fixtures, and never load/download upstream Geth.
func TestUpstreamSigningRLPAndCreateAddresses(t *testing.T) {
	data, err := os.ReadFile("testdata/ethereum-signing.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors struct {
		Source       string
		Transactions []struct {
			Type                            uint8
			Wire, Hash, SigningHash, Sender string
		}
		Create map[string]string
	}
	if err = json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	h := setup(t)
	signer := types.NewCancunSigner(big.NewInt(1337))
	if len(vectors.Transactions) != 4 || len(vectors.Create) != 7 {
		t.Fatal("missing reference vectors")
	}
	for _, v := range vectors.Transactions {
		t.Run(strconv.Itoa(int(v.Type)), func(t *testing.T) {
			wire, err := hex.DecodeString(v.Wire)
			if err != nil {
				t.Fatal(err)
			}
			var tx types.Transaction
			if err = tx.UnmarshalBinary(wire); err != nil {
				t.Fatal(err)
			}
			if tx.Type() != v.Type || tx.Hash() != common.HexToHash(v.Hash) || signer.Hash(&tx) != common.HexToHash(v.SigningHash) {
				t.Fatal("reference hash mismatch")
			}
			from, err := types.Sender(signer, &tx)
			if err != nil || from != common.HexToAddress(v.Sender) {
				t.Fatal("reference sender mismatch", err)
			}
			encoded, err := tx.MarshalBinary()
			if err != nil || !bytes.Equal(encoded, wire) || tx.Size() != uint64(len(wire)) {
				t.Fatal("RLP round trip/size", err)
			}
			signed, err := types.SignTx(&tx, signer, h.key)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err = signed.MarshalBinary()
			if err != nil || !bytes.Equal(encoded, wire) {
				t.Fatal("signature changed", err)
			}
			if v.Type == types.BlobTxType {
				network, err := rlp.EncodeToBytes([]any{rlp.RawValue(wire[1:]), []any{}, []any{}, []any{}})
				if err != nil {
					t.Fatal(err)
				}
				var unsupported types.Transaction
				if err = unsupported.UnmarshalBinary(append([]byte{types.BlobTxType}, network...)); err == nil {
					t.Fatal("network sidecar envelope accepted")
				}
			}
		})
	}
	for nonce, expected := range vectors.Create {
		n, err := strconv.ParseUint(nonce, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		if crypto.CreateAddress(h.sender, n) != common.HexToAddress(expected) {
			t.Fatal("CREATE address mismatch", nonce)
		}
	}
}
