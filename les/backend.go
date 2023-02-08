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

// Package les implements the Light Abeychain Subprotocol.
package les

import (
	"fmt"
	"github.com/abeychain/go-abey/abey/fastdownloader"
	"sync"
	"time"

	"github.com/abeychain/go-abey/abey"
	"github.com/abeychain/go-abey/abey/filters"
	"github.com/abeychain/go-abey/abey/gasprice"
	"github.com/abeychain/go-abey/accounts"
	"github.com/abeychain/go-abey/common"
	"github.com/abeychain/go-abey/common/hexutil"
	"github.com/abeychain/go-abey/consensus"
	"github.com/abeychain/go-abey/core"
	"github.com/abeychain/go-abey/core/bloombits"
	"github.com/abeychain/go-abey/core/rawdb"
	"github.com/abeychain/go-abey/core/types"
	"github.com/abeychain/go-abey/event"
	"github.com/abeychain/go-abey/internal/abeyapi"
	"github.com/abeychain/go-abey/light"
	"github.com/abeychain/go-abey/log"
	"github.com/abeychain/go-abey/node"
	"github.com/abeychain/go-abey/p2p"
	"github.com/abeychain/go-abey/p2p/discv5"
	"github.com/abeychain/go-abey/params"
	"github.com/abeychain/go-abey/rpc"
)

type LightAbey struct {
	lesCommons

	odr         *LesOdr
	relay       *LesTxRelay
	chainConfig *params.ChainConfig
	// Channel for shutting down the service
	shutdownChan chan bool

	// Handlers
	peers      *peerSet
	txPool     *light.TxPool
	election   *Election
	blockchain *light.LightChain
	serverPool *serverPool
	reqDist    *requestDistributor
	retriever  *retrieveManager

	bloomRequests chan chan *bloombits.Retrieval // Channel receiving bloom data retrieval requests
	bloomIndexer  *core.ChainIndexer

	ApiBackend *LesApiBackend

	eventMux       *event.TypeMux
	engine         consensus.Engine
	accountManager *accounts.Manager

	networkId     uint64
	netRPCService *abeyapi.PublicNetAPI

	genesisHash common.Hash
	wg          sync.WaitGroup
}

func New(ctx *node.ServiceContext, config *abey.Config) (*LightAbey, error) {
	chainDb, err := abey.CreateDB(ctx, config, "lightchaindata")
	if err != nil {
		return nil, err
	}
	chainConfig, genesisHash, genesisErr := core.SetupGenesisBlockForLes(chainDb, config.Genesis)
	if genesisErr != nil {
		return nil, genesisErr
	}
	log.Info("Initialised chain configuration", "config", chainConfig)

	peers := newPeerSet()
	quitSync := make(chan struct{})

	labey := &LightAbey{
		lesCommons: lesCommons{
			chainDb: chainDb,
			config:  config,
			iConfig: light.DefaultClientIndexerConfig,
		},
		genesisHash:    genesisHash,
		chainConfig:    chainConfig,
		eventMux:       ctx.EventMux,
		peers:          peers,
		reqDist:        newRequestDistributor(peers, quitSync),
		accountManager: ctx.AccountManager,
		engine:         abey.CreateConsensusEngine(ctx, &config.MinervaHash, chainConfig, chainDb),
		shutdownChan:   make(chan bool),
		networkId:      config.NetworkId,
		bloomRequests:  make(chan chan *bloombits.Retrieval),
		bloomIndexer:   abey.NewBloomIndexer(chainDb, params.BloomBitsBlocksClient, params.HelperTrieConfirmations),
	}

	labey.relay = NewLesTxRelay(peers, labey.reqDist)
	labey.serverPool = newServerPool(chainDb, quitSync, &labey.wg, nil)
	labey.retriever = newRetrieveManager(peers, labey.reqDist, labey.serverPool)

	labey.odr = NewLesOdr(chainDb, light.DefaultClientIndexerConfig, labey.retriever)
	labey.chtIndexer = light.NewChtIndexer(chainDb, labey.odr, params.CHTFrequencyClient, params.HelperTrieConfirmations)
	labey.bloomTrieIndexer = light.NewBloomTrieIndexer(chainDb, labey.odr, params.BloomBitsBlocksClient, params.BloomTrieFrequency)
	labey.odr.SetIndexers(labey.chtIndexer, labey.bloomTrieIndexer, labey.bloomIndexer)

	// Note: NewLightChain adds the trusted checkpoint so it needs an ODR with
	// indexers already set but not started yet
	// TODO make the params.MainnetTrustedCheckpoint in the config
	if labey.blockchain, err = light.NewLightChain(labey.odr, labey.chainConfig, labey.engine, params.MainnetTrustedCheckpoint); err != nil {
		return nil, err
	}
	labey.election = NewLightElection(labey.blockchain)
	labey.engine.SetElection(labey.election)
	// Note: AddChildIndexer starts the update process for the child
	labey.bloomIndexer.AddChildIndexer(labey.bloomTrieIndexer)
	labey.chtIndexer.Start(labey.blockchain)
	labey.bloomIndexer.Start(labey.blockchain)

	// Rewind the chain in case of an incompatible config upgrade.
	if compat, ok := genesisErr.(*params.ConfigCompatError); ok {
		log.Warn("Rewinding chain to upgrade configuration", "err", compat)
		labey.blockchain.SetHead(compat.RewindTo)
		rawdb.WriteChainConfig(chainDb, genesisHash, chainConfig)
	}

	labey.txPool = light.NewTxPool(labey.chainConfig, labey.blockchain, labey.relay)
	if labey.protocolManager, err = NewProtocolManager(labey.chainConfig, light.DefaultClientIndexerConfig, true,
		config.NetworkId, labey.eventMux, labey.engine, labey.peers, labey.blockchain, nil,
		chainDb, labey.odr, labey.relay, labey.serverPool, quitSync, &labey.wg, labey.genesisHash); err != nil {
		return nil, err
	}
	labey.ApiBackend = &LesApiBackend{labey, nil}
	gpoParams := config.GPO
	if gpoParams.Default == nil {
		gpoParams.Default = config.GasPrice
	}
	labey.ApiBackend.gpo = gasprice.NewOracle(labey.ApiBackend, gpoParams)
	return labey, nil
}

func lesTopic(genesisHash common.Hash, protocolVersion uint) discv5.Topic {
	var name string
	switch protocolVersion {
	case lpv1:
		name = "LES"
	case lpv2:
		name = "LES2"
	default:
		panic(nil)
	}
	return discv5.Topic(name + "@" + common.Bytes2Hex(genesisHash.Bytes()[0:8]))
}

type LightDummyAPI struct{}

// Etherbase is the address that mining rewards will be send to
func (s *LightDummyAPI) Etherbase() (common.Address, error) {
	return common.Address{}, fmt.Errorf("not supported")
}

// Coinbase is the address that mining rewards will be send to (alias for Etherbase)
func (s *LightDummyAPI) Coinbase() (common.Address, error) {
	return common.Address{}, fmt.Errorf("not supported")
}

// Hashrate returns the POW hashrate
func (s *LightDummyAPI) Hashrate() hexutil.Uint {
	return 0
}

// Mining returns an indication if this node is currently mining.
func (s *LightDummyAPI) Mining() bool {
	return false
}

// APIs returns the collection of RPC services the ethereum package offers.
// NOTE, some of these services probably need to be moved to somewhere else.
func (s *LightAbey) APIs() []rpc.API {
	return append(abeyapi.GetAPIs(s.ApiBackend), []rpc.API{
		{
			Namespace: "eth",
			Version:   "1.0",
			Service:   &LightDummyAPI{},
			Public:    true,
		}, // {
		//	Namespace: "eth",
		//	Version:   "1.0",
		//	Service:   downloader.NewPublicDownloaderAPI(s.protocolManager.downloader, s.eventMux),
		//	Public:    true,
		//},
		{
			Namespace: "eth",
			Version:   "1.0",
			Service:   filters.NewPublicFilterAPI(s.ApiBackend, true),
			Public:    true,
		}, {
			Namespace: "net",
			Version:   "1.0",
			Service:   s.netRPCService,
			Public:    true,
		},
	}...)
	//return apis
}

func (s *LightAbey) ResetWithGenesisBlock(gb *types.Block) {
	s.blockchain.ResetWithGenesisBlock(gb)
}

func (s *LightAbey) SnailBlockChain() *light.LightChain     { return s.blockchain }
func (s *LightAbey) TxPool() *light.TxPool                  { return s.txPool }
func (s *LightAbey) Engine() consensus.Engine               { return s.engine }
func (s *LightAbey) LesVersion() int                        { return int(ClientProtocolVersions[0]) }
func (s *LightAbey) Downloader() *fastdownloader.Downloader { return s.protocolManager.downloader }
func (s *LightAbey) EventMux() *event.TypeMux               { return s.eventMux }

// Protocols implements node.Service, returning all the currently configured
// network protocols to start.
func (s *LightAbey) Protocols() []p2p.Protocol {
	return s.makeProtocols(ClientProtocolVersions)
}
func (s *LightAbey) GenesisHash() common.Hash {
	return s.genesisHash
}
func GenesisNumber() uint64 {
	return params.LesProtocolGenesisBlock
}

// Start implements node.Service, starting all internal goroutines needed by the
// Abeychain protocol implementation.
func (s *LightAbey) Start(srvr *p2p.Server) error {
	log.Warn("Light client mode is an experimental feature")
	s.startBloomHandlers(params.BloomBitsBlocksClient)
	s.netRPCService = abeyapi.NewPublicNetAPI(srvr, s.networkId)
	// clients are searching for the first advertised protocol in the list
	protocolVersion := AdvertiseProtocolVersions[0]
	s.serverPool.start(srvr, lesTopic(s.SnailBlockChain().Genesis().Hash(), protocolVersion))
	s.protocolManager.Start(s.config.LightPeers)
	return nil
}

// Stop implements node.Service, terminating all internal goroutines used by the
// Abeychain protocol.
func (s *LightAbey) Stop() error {
	s.odr.Stop()
	s.bloomIndexer.Close()
	s.chtIndexer.Close()
	s.blockchain.Stop()
	s.protocolManager.Stop()
	s.txPool.Stop()
	s.eventMux.Stop()

	time.Sleep(time.Millisecond * 200)
	s.chainDb.Close()
	close(s.shutdownChan)

	return nil
}
