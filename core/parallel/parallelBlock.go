package parallel

import (
	"container/list"
	"github.com/abeychain/go-abey/common"
	"github.com/abeychain/go-abey/core/state"
	"github.com/abeychain/go-abey/core/types"
)

type ParallelBlock struct {
	header                     *types.Header
	transactions               types.Transactions
	executionGroups            map[int]*ExecutionGroup
	associatedAddressMap       map[common.Address]*TouchedAddressObject
	conflictIdsGroups          []map[int]struct{}
	conflictTrxPos             map[int]bool
	newGroups                  map[int]*ExecutionGroup
	trxHashToTouchedAddressMap map[common.Hash]*TouchedAddressObject
	trxHashToIndexMap          map[common.Hash]int
	trxHashToMsgMap            map[common.Hash]*types.Message
	trxHashToGroupIdMap        map[common.Hash]int
	nextGroupId                int
	statedb                    *state.StateDB
}

func NewParallelBlock(block *types.Block, statedb *state.StateDB) *ParallelBlock {
	return &ParallelBlock{header: block.Header(), transactions: block.Transactions(), statedb: statedb}
}
func (pb *ParallelBlock) Group() map[int]*ExecutionGroup {
	pb.newGroups = make(map[int]*ExecutionGroup)
	tmpExecutionGroupMap := pb.groupTransactions(pb.transactions, false)

	for _, execGroup := range tmpExecutionGroupMap {
		execGroup.sortTrxByIndex(pb.trxHashToIndexMap)
		pb.nextGroupId++
		execGroup.setId(pb.nextGroupId)
		pb.executionGroups[pb.nextGroupId] = execGroup
		pb.newGroups[pb.nextGroupId] = execGroup
	}

	return pb.newGroups
}

func (pb *ParallelBlock) ReGroup() {
	pb.newGroups = make(map[int]*ExecutionGroup)

	for _, conflictGroups := range pb.conflictIdsGroups {
		executionGroup := NewExecutionGroup()
		originTrxHashToGroupMap := make(map[common.Hash]int)
		for groupId, _ := range conflictGroups {
			executionGroup.AddTransactions(pb.executionGroups[groupId].Transactions())

			for _, trx := range pb.executionGroups[groupId].Transactions() {
				originTrxHashToGroupMap[trx.Hash()] = groupId
			}
		}
		executionGroup.sortTrxByIndex(pb.trxHashToIndexMap)

		tmpExecGroupMap := pb.groupTransactions(executionGroup.Transactions(), true)

		for _, group := range tmpExecGroupMap {
			conflict := false
			group.sortTrxByIndex(pb.trxHashToIndexMap)
			for _, trx := range group.transactions {
				trxHash := trx.Hash()
				oldGroupId := originTrxHashToGroupMap[trxHash]
				if !conflict {
					trxIndex := pb.trxHashToIndexMap[trxHash]
					if _, ok := pb.conflictTrxPos[trxIndex]; ok {
						conflict = true
						group.setStartTrxPos(trxHash, trxIndex)
					} else {
						group.addTrxHashToGetPartResult(oldGroupId, trxHash)
					}
				}
				group.addTrxToRollbackInOtherGroup(oldGroupId, trxHash)
			}

			if conflict {
				pb.nextGroupId++
				group.setId(pb.nextGroupId)
				pb.executionGroups[pb.nextGroupId] = group
				pb.newGroups[pb.nextGroupId] = group
			}
		}
	}
}
func (pb *ParallelBlock) groupTransactions(transactions types.Transactions, regroup bool) map[int]*ExecutionGroup {
	executionGroupMap := make(map[int]*ExecutionGroup)
	accountWrited := make(map[common.Address]bool)
	storageWrited := make(map[StorageAddress]bool)
	groupTouchedAccountMap := make(map[int]map[common.Address]bool)
	groupTouchedStorageMap := make(map[int]map[StorageAddress]bool)
	groupId := 0

	for _, trx := range transactions {
		groupsToMerge := make(map[int]bool)
		groupTouchedAccount := make(map[common.Address]bool)
		groupTouchedStorage := make(map[StorageAddress]bool)
		trxTouchedAddres := pb.getTrxTouchedAddress(trx.Hash(), regroup)

		for addr, op := range trxTouchedAddres.AccountOp() {
			if _, ok := accountWrited[addr]; ok {
				for gId, addrs := range groupTouchedAccountMap {
					if _, ok := groupsToMerge[gId]; ok {
						continue
					}

					if _, ok := addrs[addr]; ok {
						groupsToMerge[gId] = true
					}
				}
			} else if op {
				accountWrited[addr] = true
			}

			if op {
				groupTouchedAccount[addr] = true
			}
		}

		for storage, op := range trxTouchedAddres.StorageOp() {
			if _, ok := storageWrited[storage]; ok {
				for gId, storages := range groupTouchedStorageMap {
					if _, ok := groupsToMerge[gId]; ok {
						continue
					}

					if _, ok := storages[storage]; ok {
						groupsToMerge[gId] = true
					}
				}
			} else if op {
				storageWrited[storage] = true
			}

			if op {
				groupTouchedStorage[storage] = true
			}
		}

		tmpExecutionGroup := NewExecutionGroup()
		tmpExecutionGroup.AddTransaction(trx)
		tmpExecutionGroup.SetHeader(pb.header)
		for gId := range groupsToMerge {
			tmpExecutionGroup.AddTransactions(executionGroupMap[gId].Transactions())
			delete(executionGroupMap, gId)
			for k, v := range groupTouchedAccountMap[gId] {
				groupTouchedAccount[k] = v
			}
			delete(groupTouchedAccountMap, gId)
			for k, v := range groupTouchedStorageMap[gId] {
				groupTouchedStorage[k] = v
			}
			delete(groupTouchedStorageMap, gId)
		}

		groupId++
		groupTouchedAccountMap[groupId] = groupTouchedAccount
		groupTouchedStorageMap[groupId] = groupTouchedStorage
		executionGroupMap[groupId] = tmpExecutionGroup
	}

	return executionGroupMap
}
