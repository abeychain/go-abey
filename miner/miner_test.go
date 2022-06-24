package miner

import (
	"errors"
	"github.com/abeychain/go-abey/abeydb"
	"github.com/abeychain/go-abey/accounts"
	"github.com/abeychain/go-abey/common"
	"github.com/abeychain/go-abey/consensus"
	"github.com/abeychain/go-abey/consensus/minerva"
	"github.com/abeychain/go-abey/core"
	"github.com/abeychain/go-abey/core/snailchain"
	"github.com/abeychain/go-abey/core/types"
	"github.com/abeychain/go-abey/core/vm"
	"github.com/abeychain/go-abey/params"
	"log"
	"math/big"
	"testing"
	"time"
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

func newMockBackend(fastchaincfg *params.ChainConfig, engine consensus.Engine) *mockBackend {
	var (
		db      = abeydb.NewMemDatabase()
		genesis = core.DefaultDevGenesisBlock()
		cache   = &core.CacheConfig{}
		vmcfg   = vm.Config{}
		//fastchaincfg = params.DevnetChainConfig
		//engine       = minerva.NewFaker()
		fastNums = 10 * params.MinimumFruits
	)
	// make fast chain
	fchain, err := core.NewBlockChain(db, cache, fastchaincfg, engine, vmcfg)
	if err != nil {
		log.Fatalf("failed to make new fast chain %v", err)
	}
	// make fast blocks
	fastGenesis := genesis.MustFastCommit(db)
	fastblocks, _ := core.GenerateChain(params.TestChainConfig, fastGenesis, engine, db, fastNums, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{0: byte(1), 19: byte(i)})
	})
	fchain.InsertChain(fastblocks)

	// make the snail chain
	snailGenesis := genesis.MustSnailCommit(db)
	schain, err := snailchain.NewSnailBlockChain(db, fastchaincfg, engine, fchain)
	if err != nil {
		log.Fatalf("failed to make new snail chain %v", err)
	}
	if _, err := schain.InsertChain(types.SnailBlocks{snailGenesis}); err != nil {
		log.Fatalf("failed to insert genesis block %v", err)
	}
	//_, err := MakeSnailBlockBlockChain(snailChain, fastchain, snailGenesis, snailBlockNumbers, 1)
	//if err != nil {
	//	utils.Fatalf("failed to make new snail blocks %v", err)
	//}
	return &mockBackend{
		db:        db,
		schain:    schain,
		fchain:    fchain,
		snailPool: snailchain.NewSnailPool(snailchain.DefaultSnailPoolConfig, fchain, schain, engine),
	}
}
func (b *mockBackend) SnailBlockChain() *snailchain.SnailBlockChain { return b.schain }
func (b *mockBackend) AccountManager() *accounts.Manager            { return b.accountManager }
func (b *mockBackend) SnailGenesis() *types.SnailBlock              { return b.schain.GetBlockByNumber(0) }
func (b *mockBackend) TxPool() *core.TxPool                         { return b.txPool }
func (b *mockBackend) BlockChain() *core.BlockChain                 { return b.fchain }
func (b *mockBackend) ChainDb() abeydb.Database                     { return b.db }
func (b *mockBackend) SnailPool() *snailchain.SnailPool             { return b.snailPool }

func makeFruits(back *mockBackend, count uint64, fastchaincfg *params.ChainConfig) error {
	fcount := back.BlockChain().CurrentBlock().Number().Uint64()
	if count > fcount {
		return errors.New("count is too large")
	}
	fruits := []*types.SnailBlock{}
	for i := uint64(1); i < fcount; i++ {
		fruitHead := &types.SnailHeader{
			ParentHash: back.BlockChain().GetBlockByNumber(i - 1).Hash(),
			Publickey:  []byte{0},
			Number:     big.NewInt(int64(i)),
			Extra:      []byte{0},
			Time:       big.NewInt(time.Now().Unix()),
		}
		fruit := types.NewSnailBlock(fruitHead, []*types.SnailBlock{}, nil, nil, fastchaincfg)
		fruits = append(fruits, fruit)
	}
	errs := back.SnailPool().AddRemoteFruits(fruits, true)
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

func TestMakeSnailBlock(t *testing.T) {
	// make
	var (
		fastchaincfg = params.DevnetChainConfig
		engine       = minerva.NewFaker()
	)

	backend := newMockBackend(fastchaincfg, engine)
	worker := newWorker(fastchaincfg, engine, coinbase, backend, nil)

	// make the fruits
	err := makeFruits(backend, 60, fastchaincfg)
	if err != nil {
		log.Fatalln(err)
	}
	worker.commitNewWork()
}
