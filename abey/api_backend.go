// Copyright 2015 The go-ethereum Authors
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

package abey

import (
	"context"
	"errors"
	"math/big"

	"github.com/abeychain/go-abey/accounts"
	"github.com/abeychain/go-abey/common"
	"github.com/abeychain/go-abey/common/math"
	"github.com/abeychain/go-abey/core"
	"github.com/abeychain/go-abey/core/bloombits"
	"github.com/abeychain/go-abey/core/rawdb"
	"github.com/abeychain/go-abey/core/state"
	"github.com/abeychain/go-abey/core/types"
	"github.com/abeychain/go-abey/core/vm"
	"github.com/abeychain/go-abey/abey/downloader"
	"github.com/abeychain/go-abey/abey/gasprice"
	"github.com/abeychain/go-abey/abeydb"
	"github.com/abeychain/go-abey/event"
	"github.com/abeychain/go-abey/params"
	"github.com/abeychain/go-abey/rpc"
)

// ABEYAPIBackend implements ethapi.Backend for full nodes
type ABEYAPIBackend struct {
	abey *Abeychain
	gpo   *gasprice.Oracle
}

// ChainConfig returns the active chain configuration.
func (b *ABEYAPIBackend) ChainConfig() *params.ChainConfig {
	return b.abey.chainConfig
}

// CurrentBlock return the fast chain current Block
func (b *ABEYAPIBackend) CurrentBlock() *types.Block {
	return b.abey.blockchain.CurrentBlock()
}

// CurrentSnailBlock return the Snail chain current Block
func (b *ABEYAPIBackend) CurrentSnailBlock() *types.SnailBlock {
	return b.abey.snailblockchain.CurrentBlock()
}

// SetHead Set the newest position of Fast Chain, that will reset the fast blockchain comment
func (b *ABEYAPIBackend) SetHead(number uint64) {
	b.abey.protocolManager.downloader.Cancel()
	b.abey.blockchain.SetHead(number)
}

// SetSnailHead Set the newest position of snail chain
func (b *ABEYAPIBackend) SetSnailHead(number uint64) {
	b.abey.protocolManager.downloader.Cancel()
	b.abey.snailblockchain.SetHead(number)
}

// HeaderByNumber returns Header of fast chain by the number
// rpc.PendingBlockNumber == "pending"; rpc.LatestBlockNumber == "latest" ; rpc.LatestBlockNumber == "earliest"
func (b *ABEYAPIBackend) HeaderByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*types.Header, error) {
	// Pending block is only known by the miner
	if blockNr == rpc.PendingBlockNumber {
		block := b.abey.miner.PendingBlock()
		return block.Header(), nil
	}
	// Otherwise resolve and return the block
	if blockNr == rpc.LatestBlockNumber {
		return b.abey.blockchain.CurrentBlock().Header(), nil
	}
	return b.abey.blockchain.GetHeaderByNumber(uint64(blockNr)), nil
}

// HeaderByHash returns header of fast chain by the hash
func (b *ABEYAPIBackend) HeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error) {
	return b.abey.blockchain.GetHeaderByHash(hash), nil
}

// SnailHeaderByNumber returns Header of snail chain by the number
// rpc.PendingBlockNumber == "pending"; rpc.LatestBlockNumber == "latest" ; rpc.LatestBlockNumber == "earliest"
func (b *ABEYAPIBackend) SnailHeaderByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*types.SnailHeader, error) {
	// Pending block is only known by the miner
	if blockNr == rpc.PendingBlockNumber {
		block := b.abey.miner.PendingSnailBlock()
		return block.Header(), nil
	}
	// Otherwise resolve and return the block
	if blockNr == rpc.LatestBlockNumber {
		return b.abey.snailblockchain.CurrentBlock().Header(), nil
	}
	return b.abey.snailblockchain.GetHeaderByNumber(uint64(blockNr)), nil
}

// BlockByNumber returns block of fast chain by the number
func (b *ABEYAPIBackend) BlockByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*types.Block, error) {
	// Only snailchain has miner, also return current block here for fastchain
	if blockNr == rpc.PendingBlockNumber {
		block := b.abey.blockchain.CurrentBlock()
		return block, nil
	}
	// Otherwise resolve and return the block
	if blockNr == rpc.LatestBlockNumber {
		return b.abey.blockchain.CurrentBlock(), nil
	}
	return b.abey.blockchain.GetBlockByNumber(uint64(blockNr)), nil
}

// SnailBlockByNumber returns block of snial chain by the number
func (b *ABEYAPIBackend) SnailBlockByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*types.SnailBlock, error) {
	// Pending block is only known by the miner
	if blockNr == rpc.PendingBlockNumber {
		block := b.abey.miner.PendingSnailBlock()
		return block, nil
	}
	// Otherwise resolve and return the block
	if blockNr == rpc.LatestBlockNumber {
		return b.abey.snailblockchain.CurrentBlock(), nil
	}
	return b.abey.snailblockchain.GetBlockByNumber(uint64(blockNr)), nil
}
func (b *TrueAPIBackend) StateAndHeaderByNumberOrHash(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*state.StateDB, *types.Header, error) {
	if blockNr, ok := blockNrOrHash.Number(); ok {
		return b.StateAndHeaderByNumber(ctx, blockNr)
	}
	if hash, ok := blockNrOrHash.Hash(); ok {
		return b.StateAndHeaderByHash(ctx,hash)
	}
	return nil, nil, errors.New("invalid arguments; neither block nor hash specified")
}
// StateAndHeaderByNumber returns the state of block by the number
func (b *ABEYAPIBackend) StateAndHeaderByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*state.StateDB, *types.Header, error) {
	// Pending state is only known by the miner
	if blockNr == rpc.PendingBlockNumber {
		state, _ := b.abey.blockchain.State()
		block := b.abey.blockchain.CurrentBlock()
		return state, block.Header(), nil
	}
	// Otherwise resolve the block number and return its state
	header, err := b.HeaderByNumber(ctx, blockNr)
	if header == nil || err != nil {
		return nil, nil, err
	}
	stateDb, err := b.abey.BlockChain().StateAt(header.Root)
	return stateDb, header, err
}
func (b *TrueAPIBackend) StateAndHeaderByHash(ctx context.Context, hash common.Hash) (*state.StateDB, *types.Header, error) {
	header, err := b.HeaderByHash(ctx, hash)
	if err != nil {
		return nil, nil, err
	}
	if header == nil {
		return nil, nil, errors.New("header for hash not found")
	}
	stateDb, err := b.etrue.BlockChain().StateAt(header.Root)
	return stateDb, header, err
}

// GetBlock returns the block by the block's hash
func (b *ABEYAPIBackend) GetBlock(ctx context.Context, hash common.Hash) (*types.Block, error) {
	return b.abey.blockchain.GetBlockByHash(hash), nil
}

// GetSnailBlock returns the snail block by the block's hash
func (b *ABEYAPIBackend) GetSnailBlock(ctx context.Context, hash common.Hash) (*types.SnailBlock, error) {
	return b.abey.snailblockchain.GetBlockByHash(hash), nil
}

// GetFruit returns the fruit by the block's hash
func (b *ABEYAPIBackend) GetFruit(ctx context.Context, fastblockHash common.Hash) (*types.SnailBlock, error) {
	return b.abey.snailblockchain.GetFruit(fastblockHash), nil
}

// GetReceipts returns the Receipt details by txhash
func (b *ABEYAPIBackend) GetReceipts(ctx context.Context, hash common.Hash) (types.Receipts, error) {
	if number := rawdb.ReadHeaderNumber(b.abey.chainDb, hash); number != nil {
		return rawdb.ReadReceipts(b.abey.chainDb, hash, *number), nil
	}
	return nil, nil
}

// GetLogs returns the logs by txhash
func (b *ABEYAPIBackend) GetLogs(ctx context.Context, hash common.Hash) ([][]*types.Log, error) {
	number := rawdb.ReadHeaderNumber(b.abey.chainDb, hash)
	if number == nil {
		return nil, nil
	}
	receipts := rawdb.ReadReceipts(b.abey.chainDb, hash, *number)
	if receipts == nil {
		return nil, nil
	}
	logs := make([][]*types.Log, len(receipts))
	for i, receipt := range receipts {
		logs[i] = receipt.Logs
	}
	return logs, nil
}

// GetTd returns the total diffcult with block height by blockhash
func (b *ABEYAPIBackend) GetTd(blockHash common.Hash) *big.Int {
	return b.abey.snailblockchain.GetTdByHash(blockHash)
}

// GetEVM returns the EVM
func (b *ABEYAPIBackend) GetEVM(ctx context.Context, msg core.Message, state *state.StateDB, header *types.Header, vmCfg vm.Config) (*vm.EVM, func() error, error) {
	state.SetBalance(msg.From(), math.MaxBig256)
	vmError := func() error { return nil }

	context := core.NewEVMContext(msg, header, b.abey.BlockChain(), nil, nil)
	return vm.NewEVM(context, state, b.abey.chainConfig, vmCfg), vmError, nil
}

// SubscribeRemovedLogsEvent registers a subscription of RemovedLogsEvent in fast blockchain
func (b *ABEYAPIBackend) SubscribeRemovedLogsEvent(ch chan<- types.RemovedLogsEvent) event.Subscription {
	return b.abey.BlockChain().SubscribeRemovedLogsEvent(ch)
}

// SubscribeChainEvent registers a subscription of chainEvnet in fast blockchain
func (b *ABEYAPIBackend) SubscribeChainEvent(ch chan<- types.FastChainEvent) event.Subscription {
	return b.abey.BlockChain().SubscribeChainEvent(ch)
}

// SubscribeChainHeadEvent registers a subscription of chainHeadEvnet in fast blockchain
func (b *ABEYAPIBackend) SubscribeChainHeadEvent(ch chan<- types.FastChainHeadEvent) event.Subscription {
	return b.abey.BlockChain().SubscribeChainHeadEvent(ch)
}

// SubscribeChainSideEvent registers a subscription of chainSideEvnet in fast blockchain,deprecated
func (b *ABEYAPIBackend) SubscribeChainSideEvent(ch chan<- types.FastChainSideEvent) event.Subscription {
	return b.abey.BlockChain().SubscribeChainSideEvent(ch)
}

// SubscribeLogsEvent registers a subscription of log in fast blockchain
func (b *ABEYAPIBackend) SubscribeLogsEvent(ch chan<- []*types.Log) event.Subscription {
	return b.abey.BlockChain().SubscribeLogsEvent(ch)
}

// GetReward returns the Reward info by number in fastchain
func (b *ABEYAPIBackend) GetReward(number int64) *types.BlockReward {
	if number < 0 {
		return b.abey.blockchain.CurrentReward()
	}
	return b.abey.blockchain.GetBlockReward(uint64(number))
}

// GetSnailRewardContent returns the Reward content by number in Snailchain
func (b *ABEYAPIBackend) GetSnailRewardContent(snailNumber rpc.BlockNumber) *types.SnailRewardContenet {
	return b.abey.agent.GetSnailRewardContent(uint64(snailNumber))
}

func (b *ABEYAPIBackend) GetChainRewardContent(blockNr rpc.BlockNumber) *types.ChainReward {
	sheight := uint64(blockNr)
	return b.abey.blockchain.GetRewardInfos(sheight)
}

// GetStateChangeByFastNumber returns the Committee info by committee number
func (b *ABEYAPIBackend) GetStateChangeByFastNumber(fastNumber rpc.BlockNumber) *types.BlockBalance {
	return b.abey.blockchain.GetBalanceInfos(uint64(fastNumber))
}

func (b *ABEYAPIBackend) GetBalanceChangeBySnailNumber(snailNumber rpc.BlockNumber) *types.BalanceChangeContent {
	var sBlock = b.abey.SnailBlockChain().GetBlockByNumber(uint64(snailNumber))
	state, _ := b.abey.BlockChain().State()
	var (
		addrWithBalance          = make(map[common.Address]*big.Int)
		committeeAddrWithBalance = make(map[common.Address]*big.Int)
		blockFruits              = sBlock.Body().Fruits
		blockFruitsLen           = big.NewInt(int64(len(blockFruits)))
	)
	if blockFruitsLen.Uint64() == 0 {
		return nil
	}
	//snailBlock miner's award
	var balance = state.GetBalance(sBlock.Coinbase())
	addrWithBalance[sBlock.Coinbase()] = balance

	for _, fruit := range blockFruits {
		if addrWithBalance[fruit.Coinbase()] == nil {
			addrWithBalance[fruit.Coinbase()] = state.GetBalance(fruit.Coinbase())
		}
		var committeeMembers = b.abey.election.GetCommittee(fruit.FastNumber())

		for _, cm := range committeeMembers {
			if committeeAddrWithBalance[cm.Coinbase] == nil {
				committeeAddrWithBalance[cm.Coinbase] = state.GetBalance(cm.Coinbase)
			}
		}
	}
	for addr, balance := range committeeAddrWithBalance {
		if addrWithBalance[addr] == nil {
			addrWithBalance[addr] = balance
		}
	}
	return &types.BalanceChangeContent{addrWithBalance}
}

func (b *ABEYAPIBackend) GetCommittee(number rpc.BlockNumber) (map[string]interface{}, error) {
	if number == rpc.LatestBlockNumber {
		return b.abey.election.GetCommitteeById(new(big.Int).SetUint64(b.abey.agent.CommitteeNumber())), nil
	}
	return b.abey.election.GetCommitteeById(big.NewInt(number.Int64())), nil
}

func (b *ABEYAPIBackend) GetCurrentCommitteeNumber() *big.Int {
	return b.abey.election.GetCurrentCommitteeNumber()
}

// SendTx returns nil by success to add local txpool
func (b *ABEYAPIBackend) SendTx(ctx context.Context, signedTx *types.Transaction) error {
	return b.abey.txPool.AddLocal(signedTx)
}

// GetPoolTransactions returns Transactions by pending state in txpool
func (b *ABEYAPIBackend) GetPoolTransactions() (types.Transactions, error) {
	pending, err := b.abey.txPool.Pending()
	if err != nil {
		return nil, err
	}
	var txs types.Transactions
	for _, batch := range pending {
		txs = append(txs, batch...)
	}
	return txs, nil
}

// GetPoolTransaction returns Transaction by txHash in txpool
func (b *ABEYAPIBackend) GetPoolTransaction(hash common.Hash) *types.Transaction {
	return b.abey.txPool.Get(hash)
}

// GetPoolNonce returns user nonce by user address in txpool
func (b *ABEYAPIBackend) GetPoolNonce(ctx context.Context, addr common.Address) (uint64, error) {
	return b.abey.txPool.State().GetNonce(addr), nil
}

// Stats returns the count tx in txpool
func (b *ABEYAPIBackend) Stats() (pending int, queued int) {
	return b.abey.txPool.Stats()
}

func (b *ABEYAPIBackend) TxPoolContent() (map[common.Address]types.Transactions, map[common.Address]types.Transactions) {
	return b.abey.TxPool().Content()
}

// SubscribeNewTxsEvent returns the subscript event of new tx
func (b *ABEYAPIBackend) SubscribeNewTxsEvent(ch chan<- types.NewTxsEvent) event.Subscription {
	return b.abey.TxPool().SubscribeNewTxsEvent(ch)
}

// Downloader returns the fast downloader
func (b *ABEYAPIBackend) Downloader() *downloader.Downloader {
	return b.abey.Downloader()
}

// ProtocolVersion returns the version of protocol
func (b *ABEYAPIBackend) ProtocolVersion() int {
	return b.abey.EthVersion()
}

// SuggestPrice returns tht suggest gas price
func (b *ABEYAPIBackend) SuggestPrice(ctx context.Context) (*big.Int, error) {
	return b.gpo.SuggestPrice(ctx)
}

// ChainDb returns tht database of fastchain
func (b *ABEYAPIBackend) ChainDb() abeydb.Database {
	return b.abey.ChainDb()
}

// EventMux returns Event locker
func (b *ABEYAPIBackend) EventMux() *event.TypeMux {
	return b.abey.EventMux()
}

// AccountManager returns Account Manager
func (b *ABEYAPIBackend) AccountManager() *accounts.Manager {
	return b.abey.AccountManager()
}

// SnailPoolContent returns snail pool content
func (b *ABEYAPIBackend) SnailPoolContent() []*types.SnailBlock {
	return b.abey.SnailPool().Content()
}

// SnailPoolInspect returns snail pool Inspect
func (b *ABEYAPIBackend) SnailPoolInspect() []*types.SnailBlock {
	return b.abey.SnailPool().Inspect()
}

// SnailPoolStats returns snail pool Stats
func (b *ABEYAPIBackend) SnailPoolStats() (pending int, unVerified int) {
	return b.abey.SnailPool().Stats()
}

// BloomStatus returns Bloom Status
func (b *ABEYAPIBackend) BloomStatus() (uint64, uint64) {
	sections, _, _ := b.abey.bloomIndexer.Sections()
	return params.BloomBitsBlocks, sections
}

// ServiceFilter make the Filter for the truechian
func (b *ABEYAPIBackend) ServiceFilter(ctx context.Context, session *bloombits.MatcherSession) {
	for i := 0; i < bloomFilterThreads; i++ {
		go session.Multiplex(bloomRetrievalBatch, bloomRetrievalWait, b.abey.bloomRequests)
	}
}
