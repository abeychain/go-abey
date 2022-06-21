package miner

import (
	"github.com/abeychain/go-abey/abeydb"
	"github.com/abeychain/go-abey/accounts"
	"github.com/abeychain/go-abey/consensus"
	"github.com/abeychain/go-abey/consensus/minerva"
	"github.com/abeychain/go-abey/core"
	"github.com/abeychain/go-abey/core/snailchain"
	"github.com/abeychain/go-abey/core/types"
	"github.com/abeychain/go-abey/params"
	"testing"
)

type mockBackend struct {
	db             abeydb.Database
	txPool         *core.TxPool
	schain         *snailchain.SnailBlockChain
	fchain         *core.BlockChain
	uncleBlock     *types.Block
	snailPool      *snailchain.SnailPool
	accountManager *accounts.Manager
}

func newmockBackend(t *testing.T, chainConfig *params.ChainConfig, engine consensus.Engine, n int) *mockBackend {
	var (
		db      = abeydb.NewMemDatabase()
		genesis = core.DefaultDevGenesisBlock()
	)
	snailChainLocal, fastChainLocal = snailchain.MakeChain(fastChainHight, blockNum, genesis, minerva.NewFaker())
	//sv := snailchain.NewBlockValidator(chainConfig, fastChainLocal, snailChainLocal, engine)

	return &mockBackend{
		db:        db,
		schain:    snailChainLocal,
		fchain:    fastChainLocal,
		snailPool: snailchain.NewSnailPool(snailchain.DefaultSnailPoolConfig, fastChainLocal, snailChainLocal, engine),
	}
}

func (b *mockBackend) SnailBlockChain() *snailchain.SnailBlockChain { return b.schain }
func (b *mockBackend) AccountManager() *accounts.Manager            { return b.accountManager }
func (b *mockBackend) SnailGenesis() *types.SnailBlock              { return b.schain.GetBlockByNumber(0) }
func (b *mockBackend) TxPool() *core.TxPool                         { return b.txPool }
func (b *mockBackend) BlockChain() *core.BlockChain                 { return b.fchain }
func (b *mockBackend) ChainDb() abeydb.Database                     { return b.db }
func (b *mockBackend) SnailPool() *snailchain.SnailPool             { return b.snailPool }

func Test01(t *testing.T) {

}
