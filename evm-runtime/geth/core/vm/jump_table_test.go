// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"testing"
)

// TestJumpTableCopy tests that deep copy is necessary to prevent modify shared jump table
func TestJumpTableCopy(t *testing.T) {
	tbl := newMergeInstructionSet()
	if tbl[SLOAD].constantGas != 0 {
		t.Fatal("shared jump table was modified")
	}

	// a deep copy won't modify the shared jump table
	deepCopy := copyJumpTable(&tbl)
	deepCopy[SLOAD].constantGas = 100
	if deepCopy[SLOAD].constantGas != 100 {
		t.Fatal("copy was not modified")
	}
	if tbl[SLOAD].constantGas != 0 {
		t.Fatal("shared jump table was modified")
	}
}
