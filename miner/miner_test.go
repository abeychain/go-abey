package miner

import (
	"errors"
	"fmt"
	"log"
	"math/big"
	"testing"
	"time"

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
	// make genesis block
	fastGenesis := genesis.MustFastCommit(db)
	// make fast chain
	fchain, err := core.NewBlockChain(db, cache, fastchaincfg, engine, vmcfg)
	if err != nil {
		log.Fatalf("failed to make new fast chain %v", err)
	}

	// make the snail chain
	snailGenesis := genesis.MustSnailCommit(db)
	schain, err := snailchain.NewSnailBlockChain(db, fastchaincfg, engine, fchain)
	if err != nil {
		log.Fatalf("failed to make new snail chain %v", err)
	}
	engine.SetSnailChainReader(schain)
	// make fast blocks
	fastblocks, _ := core.GenerateChain(fastchaincfg, fastGenesis, engine, db, fastNums, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{0: byte(1), 19: byte(i)})
	})
	fchain.InsertChain(fastblocks)

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

func makeSnailBlock(parent *types.SnailBlock) (*types.SnailBlock, error) {
	head := &types.SnailHeader{
		ParentHash: parent.Hash(),
		Publickey:  []byte{0},
		Number:     new(big.Int).Add(parent.Number(), big.NewInt(1)),
		Extra:      []byte{0},
		Time:       big.NewInt(time.Now().Unix()),
	}
	b := types.NewSnailBlock(head, []*types.SnailBlock{}, nil, nil, params.DevnetChainConfig)
	return b, nil
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

func TestStopMiningForHeight(t *testing.T) {
	backend := newMockBackend(params.DevnetChainConfig, minerva.NewFaker())
	params.StopSnailMiner = big.NewInt(1)

	for i := uint64(0); i < params.StopSnailMiner.Uint64(); i++ {
		parent := backend.SnailBlockChain().CurrentBlock()
		block, _ := makeSnailBlock(parent)
		c, err := backend.SnailBlockChain().InsertChain(types.SnailBlocks{block})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("insert snail block", c)
	}

	// make snail block after stop mining
	snailAfterStopMining := 10
	for i := 0; i < snailAfterStopMining; i++ {
		parent := backend.SnailBlockChain().CurrentBlock()
		block, _ := makeSnailBlock(parent)
		c, err := backend.SnailBlockChain().InsertChain(types.SnailBlocks{block})
		if err == nil || c > 0 {
			log.Fatal("cann't insert snail block on stop miner")
		}
	}
}

func testWorker(chainConfig *params.ChainConfig, engine consensus.Engine) (*worker, *mockBackend) {
	backend := newMockBackend(chainConfig, engine)

	w := newWorker(chainConfig, engine, coinbase, backend, nil)

	return w, backend
}

func TestStopCommitFastBlock(t *testing.T) {
	fmt.Println("it's stop more than two snail block ")
	var (
		//fruitset1 []*types.SnailBlock  // nil situation
		fruitset2 []*types.SnailBlock // contine but not have 60
		fruitset3 []*types.SnailBlock // not contine   1 2 3  5 7 8
		fruitset4 []*types.SnailBlock // contine and langer then 60
		fruitset5 []*types.SnailBlock // frist one big then snailfruitslast fast numbe 10000 10001...
	)
	engine := minerva.NewFaker()

	chainDb := abeydb.NewMemDatabase()
	chainConfig, _, _, _ := core.SetupGenesisBlock(chainDb, core.DefaultGenesisBlock())
	//Miner := New(snailChainLocal, nil, nil, snailChainLocal.Engine(), nil, false, nil)
	worker, _ := testWorker(chainConfig, engine)

	startFastNum := blockNum*params.MinimumFruits + 1
	gensisSnail := snailChainLocal.GetBlockByNumber(0)
	worker.commitNewWork()
	// situation 1   nil
	//fruitset1 = nil
	err0 := worker.CommitFastBlocksByWoker(nil, snailChainLocal, fastChainLocal, nil)
	if err0 != nil {
		fmt.Println("1 is err", err0)
	}

	// situation 2   1 2 3 4
	for i := startFastNum; i < (10 + startFastNum); i++ {

		fruit, _ := snailchain.MakeSnailBlockFruit(snailChainLocal, fastChainLocal, blockNum, i, gensisSnail.PublicKey(), gensisSnail.Coinbase(), false, nil)
		if fruit == nil {
			fmt.Println("fruit is nil  2")
		}
		fruitset2 = append(fruitset2, fruit)
	}

	err := worker.CommitFastBlocksByWoker(fruitset2, snailChainLocal, fastChainLocal, nil)
	if err != nil {
		fmt.Println("2 is err", err)
	}

	// situation 3   1 2 3 5 7
	j := 0
	for i := startFastNum; i < startFastNum+20; i++ {
		j++
		if j == 10 {
			continue
		}
		fruit, _ := snailchain.MakeSnailBlockFruit(snailChainLocal, fastChainLocal, blockNum, i, gensisSnail.PublicKey(), gensisSnail.Coinbase(), false, nil)
		if fruit == nil {
			fmt.Println("fruit is nil  3")
		}
		fruitset3 = append(fruitset3, fruit)
	}

	err2 := worker.CommitFastBlocksByWoker(fruitset3, snailChainLocal, fastChainLocal, nil)
	if err != nil {
		fmt.Println("3 is err", err2)
	}
	// situation 4   1 2 3...60
	for i := startFastNum; i < startFastNum+60; i++ {

		fruit, _ := snailchain.MakeSnailBlockFruit(snailChainLocal, fastChainLocal, blockNum, i, gensisSnail.PublicKey(), gensisSnail.Coinbase(), false, nil)
		if fruit == nil {
			fmt.Println("fruit is nil 4 ")
		}
		fruitset4 = append(fruitset4, fruit)
	}
	err3 := worker.CommitFastBlocksByWoker(fruitset4, snailChainLocal, fastChainLocal, nil)
	if err != nil {
		fmt.Println("4 is err", err3)
	}

	// situation 5   10000 10001...
	for i := fastChainHight; i < startFastNum+60; i++ {

		fruit, _ := snailchain.MakeSnailBlockFruit(snailChainLocal, fastChainLocal, blockNum, i, gensisSnail.PublicKey(), gensisSnail.Coinbase(), false, nil)
		if fruit == nil {
			fmt.Println("fruit is nil  5")
		}
		fruitset5 = append(fruitset5, fruit)
	}
	err5 := worker.CommitFastBlocksByWoker(fruitset5, snailChainLocal, fastChainLocal, nil)
	if err != nil {
		fmt.Println("5 is err", err5)
	}

	snail_blocks := snailChainLocal.GetBlocksFromNumber(1)
	for _, block := range snail_blocks {
		fmt.Printf("snail %d => %x\n", block.Number(), block.Hash())
	}

	for i := uint64(0); i <= fastChainLocal.CurrentBlock().Number().Uint64(); i++ {
		block := fastChainLocal.GetBlockByNumber(i)
		if block == nil {
			break
		} else {
			fmt.Printf("fast %d => %x\n", block.Number(), block.Hash())
		}
	}
}
