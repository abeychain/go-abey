// Copyright 2017 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

// faucet is an Ether faucet backed by a light client.

package main

import (
	"fmt"
	"github.com/abeychain/go-abey/common"
	"github.com/abeychain/go-abey/core"
)

// getGenesis returns a genesis based on input args
func getGenesis(genesisFlag string, testnetFlag bool, devnetFlag bool) (*core.Genesis, error) {
	switch {
	case genesisFlag != "":
		var genesis core.Genesis
		err := common.LoadJSON(genesisFlag, &genesis)
		return &genesis, err
	case testnetFlag:
		return core.DefaultTestnetGenesisBlock(), nil
	case devnetFlag:
		return core.DefaultDevGenesisBlock(), nil
	default:
		return nil, fmt.Errorf("no genesis flag provided")
	}
}
