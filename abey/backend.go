// Copyright 2014 The go-ethereum Authors
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

// Package abey implements the Abeychain protocol.
package abey

import (
	"errors"
	"fmt"
	"github.com/abeychain/go-abey/consensus/tbft"
	config "github.com/abeychain/go-abey/params"
	"math/big"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/abeychain/go-abey/abey/downloader"
	"github.com/abeychain/go-abey/abey/filters"
	"github.com/abeychain/go-abey/abey/gasprice"
	"github.com/abeychain/go-abey/abeydb"
	"github.com/abeychain/go-abey/accounts"
	"github.com/abeychain/go-abey/common"
	"github.com/abeychain/go-abey/common/hexutil"
	"github.com/abeychain/go-abey/consensus"
	elect "github.com/abeychain/go-abey/consensus/election"
	ethash "github.com/abeychain/go-abey/consensus/minerva"
	"github.com/abeychain/go-abey/core"
	"github.com/abeychain/go-abey/core/bloombits"
	chain "github.com/abeychain/go-abey/core/snailchain"
	"github.com/abeychain/go-abey/core/snailchain/rawdb"
	"github.com/abeychain/go-abey/core/types"
	"github.com/abeychain/go-abey/core/vm"
	"github.com/abeychain/go-abey/crypto"
	"github.com/abeychain/go-abey/event"
	"github.com/abeychain/go-abey/internal/abeyapi"
	"github.com/abeychain/go-abey/log"
	"github.com/abeychain/go-abey/miner"
	"github.com/abeychain/go-abey/node"
	"github.com/abeychain/go-abey/p2p"
	"github.com/abeychain/go-abey/params"
	"github.com/abeychain/go-abey/rlp"
	"github.com/abeychain/go-abey/rpc"
)

type LesServer interface {
	Start(srvr *p2p.Server)
	Stop()
	Protocols() []p2p.Protocol
	SetBloomBitsIndexer(bbIndexer *core.ChainIndexer)
}

// Abeychain implements the Abeychain full node service.
type Abeychain struct {
	config      *Config
	chainConfig *params.ChainConfig

	// Channel for shutting down the service
	shutdownChan chan bool // Channel for shutting down the Abeychain

	// Handlers
	txPool *core.TxPool

	snailPool *chain.SnailPool

	agent    *PbftAgent
	election *elect.Election

	blockchain      *core.BlockChain
	snailblockchain *chain.SnailBlockChain
	protocolManager *ProtocolManager
	lesServer       LesServer

	// DB interfaces
	chainDb abeydb.Database // Block chain database

	eventMux       *event.TypeMux
	engine         consensus.Engine
	accountManager *accounts.Manager

	bloomRequests chan chan *bloombits.Retrieval // Channel receiving bloom data retrieval requests
	bloomIndexer  *core.ChainIndexer             // Bloom indexer operating during block imports

	APIBackend *ABEYAPIBackend

	miner     *miner.Miner
	gasPrice  *big.Int
	etherbase common.Address

	networkID     uint64
	netRPCService *abeyapi.PublicNetAPI

	pbftServer *tbft.Node

	lock sync.RWMutex // Protects the variadic fields (e.g. gas price and etherbase)
}

func (s *Abeychain) AddLesServer(ls LesServer) {
	s.lesServer = ls
	ls.SetBloomBitsIndexer(s.bloomIndexer)
}

// New creates a new Abeychain object (including the
// initialisation of the common Abeychain object)
func New(ctx *node.ServiceContext, config *Config) (*Abeychain, error) {
	if config.SyncMode == downloader.LightSync {
		return nil, errors.New("can't run abey.Abeychain in light sync mode, use les.LightAbeychain")
	}
	//if config.SyncMode == downloader.SnapShotSync {
	//	return nil, errors.New("can't run abey.Abeychain in SnapShotSync sync mode, use les.LightAbeychain")
	//}

	if !config.SyncMode.IsValid() {
		return nil, fmt.Errorf("invalid sync mode %d", config.SyncMode)
	}
	chainDb, err := CreateDB(ctx, config, "chaindata")
	//chainDb, err := CreateDB(ctx, config, path)
	if err != nil {
		return nil, err
	}

	chainConfig, genesisHash, _, genesisErr := core.SetupGenesisBlock(chainDb, config.Genesis)
	if _, ok := genesisErr.(*params.ConfigCompatError); genesisErr != nil && !ok {
		return nil, genesisErr
	}

	log.Info("Initialised chain configuration", "config", chainConfig)

	/*if config.Genesis != nil {
		config.MinerGasFloor = config.Genesis.GasLimit * 9 / 10
		config.MinerGasCeil = config.Genesis.GasLimit * 11 / 10
	}*/

	abey := &Abeychain{
		config:         config,
		chainDb:        chainDb,
		chainConfig:    chainConfig,
		eventMux:       ctx.EventMux,
		accountManager: ctx.AccountManager,
		engine:         CreateConsensusEngine(ctx, &config.MinervaHash, chainConfig, chainDb),
		shutdownChan:   make(chan bool),
		networkID:      config.NetworkId,
		gasPrice:       config.GasPrice,
		etherbase:      config.Etherbase,
		bloomRequests:  make(chan chan *bloombits.Retrieval),
		bloomIndexer:   NewBloomIndexer(chainDb, params.BloomBitsBlocks, params.BloomConfirms),
	}

	log.Info("Initialising Abeychain protocol", "versions", ProtocolVersions, "network", config.NetworkId, "syncmode", config.SyncMode)

	if !config.SkipBcVersionCheck {
		bcVersion := rawdb.ReadDatabaseVersion(chainDb)
		if bcVersion != core.BlockChainVersion && bcVersion != 0 {
			return nil, fmt.Errorf("Blockchain DB version mismatch (%d / %d). Run gabey upgradedb.\n", bcVersion, core.BlockChainVersion)
		}
		rawdb.WriteDatabaseVersion(chainDb, core.BlockChainVersion)
	}
	var (
		vmConfig    = vm.Config{EnablePreimageRecording: config.EnablePreimageRecording}
		cacheConfig = &core.CacheConfig{Deleted: config.DeletedState, Disabled: config.NoPruning, TrieNodeLimit: config.TrieCache, TrieTimeLimit: config.TrieTimeout}
	)

	abey.blockchain, err = core.NewBlockChain(chainDb, cacheConfig, abey.chainConfig, abey.engine, vmConfig)
	if err != nil {
		return nil, err
	}

	abey.snailblockchain, err = chain.NewSnailBlockChain(chainDb, abey.chainConfig, abey.engine, abey.blockchain)
	if err != nil {
		return nil, err
	}

	// Rewind the chain in case of an incompatible config upgrade.
	if compat, ok := genesisErr.(*params.ConfigCompatError); ok {
		log.Warn("Rewinding chain to upgrade configuration", "err", compat)
		abey.blockchain.SetHead(compat.RewindTo)
		rawdb.WriteChainConfig(chainDb, genesisHash, chainConfig)
	}

	//  rewind snail if case of incompatible config
	if compat, ok := genesisErr.(*params.ConfigCompatError); ok {
		log.Warn("Rewinding snail chain to upgrade configuration", "err", compat)
		abey.snailblockchain.SetHead(compat.RewindTo)
		rawdb.WriteChainConfig(chainDb, genesisHash, chainConfig)
	}

	abey.bloomIndexer.Start(abey.blockchain)

	consensus.InitTIP8(chainConfig, abey.snailblockchain)
	//sv := chain.NewBlockValidator(abey.chainConfig, abey.blockchain, abey.snailblockchain, abey.engine)
	//abey.snailblockchain.SetValidator(sv)

	if config.TxPool.Journal != "" {
		config.TxPool.Journal = ctx.ResolvePath(config.TxPool.Journal)
	}

	if config.SnailPool.Journal != "" {
		config.SnailPool.Journal = ctx.ResolvePath(config.SnailPool.Journal)
	}

	abey.txPool = core.NewTxPool(config.TxPool, abey.chainConfig, abey.blockchain)

	//abey.snailPool = chain.NewSnailPool(config.SnailPool, abey.blockchain, abey.snailblockchain, abey.engine, sv)
	abey.snailPool = chain.NewSnailPool(config.SnailPool, abey.blockchain, abey.snailblockchain, abey.engine)

	abey.election = elect.NewElection(abey.chainConfig, abey.blockchain, abey.snailblockchain, abey.config)

	//abey.snailblockchain.Validator().SetElection(abey.election, abey.blockchain)

	abey.engine.SetElection(abey.election)
	abey.engine.SetSnailChainReader(abey.snailblockchain)
	abey.election.SetEngine(abey.engine)

	//coinbase, _ := abey.Etherbase()
	abey.agent = NewPbftAgent(abey, abey.chainConfig, abey.engine, abey.election, config.MinerGasFloor, config.MinerGasCeil)
	if abey.protocolManager, err = NewProtocolManager(
		abey.chainConfig, config.SyncMode, config.NetworkId,
		abey.eventMux, abey.txPool, abey.snailPool, abey.engine,
		abey.blockchain, abey.snailblockchain,
		chainDb, abey.agent); err != nil {
		return nil, err
	}

	abey.miner = miner.New(abey, abey.chainConfig, abey.EventMux(), abey.engine, abey.election, abey.Config().MineFruit, abey.Config().NodeType, abey.Config().RemoteMine, abey.Config().Mine)
	abey.miner.SetExtra(makeExtraData(config.ExtraData))

	committeeKey, err := crypto.ToECDSA(abey.config.CommitteeKey)
	if err == nil {
		abey.miner.SetElection(abey.config.EnableElection, crypto.FromECDSAPub(&committeeKey.PublicKey))
	}

	abey.APIBackend = &ABEYAPIBackend{abey, nil}
	gpoParams := config.GPO
	if gpoParams.Default == nil {
		gpoParams.Default = config.GasPrice
	}
	abey.APIBackend.gpo = gasprice.NewOracle(abey.APIBackend, gpoParams)
	return abey, nil
}

func makeExtraData(extra []byte) []byte {
	if len(extra) == 0 {
		// create default extradata
		extra, _ = rlp.EncodeToBytes([]interface{}{
			uint(params.VersionMajor<<16 | params.VersionMinor<<8 | params.VersionPatch),
			"gabey",
			runtime.Version(),
			runtime.GOOS,
		})
	}
	if uint64(len(extra)) > params.MaximumExtraDataSize {
		log.Warn("Miner extra data exceed limit", "extra", hexutil.Bytes(extra), "limit", params.MaximumExtraDataSize)
		extra = nil
	}
	return extra
}

// CreateDB creates the chain database.
func CreateDB(ctx *node.ServiceContext, config *Config, name string) (abeydb.Database, error) {
	db, err := ctx.OpenDatabase(name, config.DatabaseCache, config.DatabaseHandles)
	if err != nil {
		return nil, err
	}
	if db, ok := db.(*abeydb.LDBDatabase); ok {
		db.Meter("abey/db/chaindata/")
	}
	return db, nil
}

// CreateConsensusEngine creates the required type of consensus engine instance for an Abeychain service
func CreateConsensusEngine(ctx *node.ServiceContext, config *ethash.Config, chainConfig *params.ChainConfig,
	db abeydb.Database) consensus.Engine {
	// If proof-of-authority is requested, set it up
	// snail chain not need clique
	/*
		if chainConfig.Clique != nil {
			return clique.New(chainConfig.Clique, db)
		}*/
	// Otherwise assume proof-of-work
	switch config.PowMode {
	case ethash.ModeFake:
		log.Info("-----Fake mode")
		log.Warn("Ethash used in fake mode")
		return ethash.NewFaker()
	case ethash.ModeTest:
		log.Warn("Ethash used in test mode")
		return ethash.NewTester()
	case ethash.ModeShared:
		log.Warn("Ethash used in shared mode")
		return ethash.NewShared()
	default:
		engine := ethash.New(ethash.Config{
			CacheDir:       ctx.ResolvePath(config.CacheDir),
			CachesInMem:    config.CachesInMem,
			CachesOnDisk:   config.CachesOnDisk,
			DatasetDir:     config.DatasetDir,
			DatasetsInMem:  config.DatasetsInMem,
			DatasetsOnDisk: config.DatasetsOnDisk,
		})
		engine.SetThreads(-1) // Disable CPU mining
		return engine
	}
}

// APIs return the collection of RPC services the abey package offers.
// NOTE, some of these services probably need to be moved to somewhere else.
func (s *Abeychain) APIs() []rpc.API {
	apis := abeyapi.GetAPIs(s.APIBackend)

	// Append any APIs exposed explicitly by the consensus engine
	apis = append(apis, s.engine.APIs(s.BlockChain())...)

	// Append abey	APIs and  Eth APIs
	namespaces := []string{"abey", "eth"}
	for _, name := range namespaces {
		apis = append(apis, []rpc.API{
			{
				Namespace: name,
				Version:   "1.0",
				Service:   NewPublicAbeychainAPI(s),
				Public:    true,
			}, {
				Namespace: name,
				Version:   "1.0",
				Service:   NewPublicMinerAPI(s),
				Public:    true,
			}, {
				Namespace: name,
				Version:   "1.0",
				Service:   downloader.NewPublicDownloaderAPI(s.protocolManager.downloader, s.eventMux),
				Public:    true,
			}, {
				Namespace: name,
				Version:   "1.0",
				Service:   filters.NewPublicFilterAPI(s.APIBackend, false),
				Public:    true,
			},
		}...)
	}
	// Append all the local APIs and return
	return append(apis, []rpc.API{
		{
			Namespace: "miner",
			Version:   "1.0",
			Service:   NewPrivateMinerAPI(s),
			Public:    false,
		}, {
			Namespace: "admin",
			Version:   "1.0",
			Service:   NewPrivateAdminAPI(s),
		}, {
			Namespace: "debug",
			Version:   "1.0",
			Service:   NewPublicDebugAPI(s),
			Public:    true,
		}, {
			Namespace: "debug",
			Version:   "1.0",
			Service:   NewPrivateDebugAPI(s.chainConfig, s),
		}, {
			Namespace: "net",
			Version:   "1.0",
			Service:   s.netRPCService,
			Public:    true,
		},
	}...)
}

func (s *Abeychain) ResetWithGenesisBlock(gb *types.Block) {
	s.blockchain.ResetWithGenesisBlock(gb)
}

func (s *Abeychain) ResetWithFastGenesisBlock(gb *types.Block) {
	s.blockchain.ResetWithGenesisBlock(gb)
}

func (s *Abeychain) Etherbase() (eb common.Address, err error) {
	s.lock.RLock()
	etherbase := s.etherbase
	s.lock.RUnlock()

	if etherbase != (common.Address{}) {
		return etherbase, nil
	}
	if wallets := s.AccountManager().Wallets(); len(wallets) > 0 {
		if accounts := wallets[0].Accounts(); len(accounts) > 0 {
			etherbase := accounts[0].Address

			s.lock.Lock()
			s.etherbase = etherbase
			s.lock.Unlock()

			log.Info("Coinbase automatically configured", "address", etherbase)
			return etherbase, nil
		}
	}
	return common.Address{}, fmt.Errorf("coinbase must be explicitly specified")
}

// SetEtherbase sets the mining reward address.
func (s *Abeychain) SetEtherbase(etherbase common.Address) {
	s.lock.Lock()
	s.etherbase = etherbase
	s.agent.committeeNode.Coinbase = etherbase
	s.lock.Unlock()

	s.miner.SetEtherbase(etherbase)
}

func (s *Abeychain) StartMining(local bool) error {
	eb, err := s.Etherbase()
	if err != nil {
		log.Error("Cannot start mining without coinbase", "err", err)
		return fmt.Errorf("coinbase missing: %v", err)
	}

	// snail chain not need clique
	/*
		if clique, ok := s.engine.(*clique.Clique); ok {
			wallet, err := s.accountManager.Find(accounts.Account{Address: eb})
			if wallet == nil || err != nil {
				log.Error("Etherbase account unavailable locally", "err", err)
				return fmt.Errorf("signer missing: %v", err)
			}
			clique.Authorize(eb, wallet.SignHash)
		}*/

	if local {
		// If local (CPU) mining is started, we can disable the transaction rejection
		// mechanism introduced to speed sync times. CPU mining on mainnet is ludicrous
		// so none will ever hit this path, whereas marking sync done on CPU mining
		// will ensure that private networks work in single miner mode too.
		atomic.StoreUint32(&s.protocolManager.acceptFruits, 1)

	}
	go s.miner.Start(eb)
	return nil
}

func (s *Abeychain) StopMining()                       { s.miner.Stop() }
func (s *Abeychain) IsMining() bool                    { return s.miner.Mining() }
func (s *Abeychain) Miner() *miner.Miner               { return s.miner }
func (s *Abeychain) PbftAgent() *PbftAgent             { return s.agent }
func (s *Abeychain) AccountManager() *accounts.Manager { return s.accountManager }
func (s *Abeychain) BlockChain() *core.BlockChain      { return s.blockchain }
func (s *Abeychain) Config() *Config                   { return s.config }

func (s *Abeychain) SnailBlockChain() *chain.SnailBlockChain { return s.snailblockchain }
func (s *Abeychain) TxPool() *core.TxPool                    { return s.txPool }

func (s *Abeychain) SnailPool() *chain.SnailPool { return s.snailPool }

func (s *Abeychain) EventMux() *event.TypeMux           { return s.eventMux }
func (s *Abeychain) Engine() consensus.Engine           { return s.engine }
func (s *Abeychain) ChainDb() abeydb.Database           { return s.chainDb }
func (s *Abeychain) IsListening() bool                  { return true } // Always listening
func (s *Abeychain) EthVersion() int                    { return int(s.protocolManager.SubProtocols[0].Version) }
func (s *Abeychain) NetVersion() uint64                 { return s.networkID }
func (s *Abeychain) Downloader() *downloader.Downloader { return s.protocolManager.downloader }
func (s *Abeychain) Synced() bool                       { return atomic.LoadUint32(&s.protocolManager.acceptTxs) == 1 }
func (s *Abeychain) ArchiveMode() bool                  { return s.config.NoPruning }

// Protocols implements node.Service, returning all the currently configured
// network protocols to start.
func (s *Abeychain) Protocols() []p2p.Protocol {
	if s.lesServer == nil {
		return s.protocolManager.SubProtocols
	}
	return append(s.protocolManager.SubProtocols, s.lesServer.Protocols()...)
}

// Start implements node.Service, starting all internal goroutines needed by the
// Abeychain protocol implementation.
func (s *Abeychain) Start(srvr *p2p.Server) error {

	// Start the bloom bits servicing goroutines
	s.startBloomHandlers()

	// Start the RPC service
	s.netRPCService = abeyapi.NewPublicNetAPI(srvr, s.NetVersion())

	// Figure out a max peers count based on the server limits
	maxPeers := srvr.MaxPeers
	if s.config.LightServ > 0 {
		if s.config.LightPeers >= srvr.MaxPeers {
			return fmt.Errorf("invalid peer config: light peer count (%d) >= total peer count (%d)", s.config.LightPeers, srvr.MaxPeers)
		}
		maxPeers -= s.config.LightPeers
	}
	// Start the networking layer and the light server if requested
	s.protocolManager.Start(maxPeers)
	s.startPbftServer()
	if s.pbftServer == nil {
		log.Error("start pbft server failed.")
		return errors.New("start pbft server failed.")
	}
	s.agent.server = s.pbftServer
	log.Info("", "server", s.agent.server)
	s.agent.Start()

	s.election.Start()

	//start fruit journal
	s.snailPool.Start()

	// Start the networking layer and the light server if requested
	s.protocolManager.Start2(maxPeers)
	if s.lesServer != nil {
		s.lesServer.Start(srvr)
	}

	return nil
}

// Stop implements node.Service, terminating all internal goroutines used by the
// Abeychain protocol.
func (s *Abeychain) Stop() error {
	s.stopPbftServer()
	s.bloomIndexer.Close()
	s.blockchain.Stop()
	s.snailblockchain.Stop()
	s.protocolManager.Stop()
	if s.lesServer != nil {
		s.lesServer.Stop()
	}
	s.txPool.Stop()
	s.snailPool.Stop()
	s.miner.Stop()
	s.eventMux.Stop()

	s.chainDb.Close()
	close(s.shutdownChan)

	return nil
}

func (s *Abeychain) startPbftServer() error {
	priv, err := crypto.ToECDSA(s.config.CommitteeKey)
	if err != nil {
		return err
	}

	cfg := config.DefaultConfig()
	cfg.P2P.ListenAddress1 = "tcp://0.0.0.0:" + strconv.Itoa(s.config.Port)
	cfg.P2P.ListenAddress2 = "tcp://0.0.0.0:" + strconv.Itoa(s.config.StandbyPort)

	n1, err := tbft.NewNode(cfg, "1", priv, s.agent)
	if err != nil {
		return err
	}
	s.pbftServer = n1
	return n1.Start()
}

func (s *Abeychain) stopPbftServer() error {
	if s.pbftServer != nil {
		s.pbftServer.Stop()
	}
	return nil
}
