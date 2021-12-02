package parallel

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/abeychain/go-abey/core/state"
	"github.com/abeychain/go-abey/core/types"
	"math/big"
)

type GroupResult struct {
	accountRecords          map[common.Address][]*AccountRecord
	storageRecords          map[StorageAddress][]*StorageRecord
	hashToCode              map[common.Hash]state.Code
}

type AccountRecord struct {
	index    int
	balance  *big.Int
	codeHash []byte
}



