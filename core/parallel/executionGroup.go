package parallel

import (
	"github.com/abeychain/go-abey/common"
	"github.com/abeychain/go-abey/core/types"
)

type ExecutionGroup struct {
	id                             int
	header                         *types.Header
	transactions                   types.Transactions
	result                         *GroupResult
	startTrxHash                   common.Hash
	startTrxIndex                  int
	trxesToGetResultFromOtherGroup map[int]map[common.Hash]struct{}
	trxesToRollBackInOtherGroup    map[int]map[common.Hash]struct{}
}

func (e *ExecutionGroup) Transactions() types.Transactions {
	return e.transactions
}

func (e *ExecutionGroup) SetTransactions(transactions types.Transactions) {
	e.transactions = transactions
}

func (e *ExecutionGroup) Result() *GroupResult {
	return e.result
}

func (e *ExecutionGroup) SetResult(result *GroupResult) {
	e.result = result
}

func (e *ExecutionGroup) Header() *types.Header {
	return e.header
}

func (e *ExecutionGroup) SetHeader(header *types.Header) {
	e.header = header
}

func (e *ExecutionGroup) AddTransaction(trx *types.Transaction) {
	e.transactions = append(e.transactions, trx)
}

func (e *ExecutionGroup) AddTransactions(transactions types.Transactions) {
	e.transactions = append(e.transactions, transactions...)
}

func NewExecutionGroup() *ExecutionGroup {
	return &ExecutionGroup{}
}


