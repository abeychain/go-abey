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

package core

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/abeychain/go-abey/core/vm"

	"github.com/abeychain/go-abey/abeydb"
	"github.com/abeychain/go-abey/common"
	"github.com/abeychain/go-abey/common/hexutil"
	"github.com/abeychain/go-abey/common/math"
	"github.com/abeychain/go-abey/consensus"
	"github.com/abeychain/go-abey/core/rawdb"
	snaildb "github.com/abeychain/go-abey/core/snailchain/rawdb"
	"github.com/abeychain/go-abey/core/state"
	"github.com/abeychain/go-abey/core/types"
	"github.com/abeychain/go-abey/crypto"
	"github.com/abeychain/go-abey/log"
	"github.com/abeychain/go-abey/params"
	"github.com/abeychain/go-abey/rlp"
)

//go:generate gencodec -type Genesis -field-override genesisSpecMarshaling -out gen_genesis.go
//go:generate gencodec -type GenesisAccount -field-override genesisAccountMarshaling -out gen_genesis_account.go

var errGenesisNoConfig = errors.New("genesis has no chain configuration")
var baseAllocamount = new(big.Int).Mul(big.NewInt(1000000), big.NewInt(1e18))

// Genesis specifies the header fields, state of a genesis block. It also defines hard
// fork switch-over blocks through the chain configuration.
type Genesis struct {
	Config     *params.ChainConfig      `json:"config"`
	Nonce      uint64                   `json:"nonce"`
	Timestamp  uint64                   `json:"timestamp"`
	ExtraData  []byte                   `json:"extraData"`
	GasLimit   uint64                   `json:"gasLimit"   gencodec:"required"`
	Difficulty *big.Int                 `json:"difficulty" gencodec:"required"`
	Mixhash    common.Hash              `json:"mixHash"`
	Coinbase   common.Address           `json:"coinbase"`
	Alloc      types.GenesisAlloc       `json:"alloc"      gencodec:"required"`
	Committee  []*types.CommitteeMember `json:"committee"      gencodec:"required"`

	// These fields are used for consensus tests. Please don't use them
	// in actual genesis blocks.
	Number     uint64      `json:"number"`
	GasUsed    uint64      `json:"gasUsed"`
	ParentHash common.Hash `json:"parentHash"`
}
type LesGenesis struct {
	Header    *types.Header            `json:"header"`
	Committee []*types.CommitteeMember `json:"committee"`
}

// GenesisAccount is an account in the state of the genesis block.
type GenesisAccount struct {
	Code       []byte                      `json:"code,omitempty"`
	Storage    map[common.Hash]common.Hash `json:"storage,omitempty"`
	Balance    *big.Int                    `json:"balance" gencodec:"required"`
	Nonce      uint64                      `json:"nonce,omitempty"`
	PrivateKey []byte                      `json:"secretKey,omitempty"` // for tests
}

// field type overrides for gencodec
type genesisSpecMarshaling struct {
	Nonce      math.HexOrDecimal64
	Timestamp  math.HexOrDecimal64
	ExtraData  hexutil.Bytes
	GasLimit   math.HexOrDecimal64
	GasUsed    math.HexOrDecimal64
	Number     math.HexOrDecimal64
	Difficulty *math.HexOrDecimal256
	Alloc      map[common.UnprefixedAddress]GenesisAccount
}

type genesisAccountMarshaling struct {
	Code       hexutil.Bytes
	Balance    *math.HexOrDecimal256
	Nonce      math.HexOrDecimal64
	Storage    map[storageJSON]storageJSON
	PrivateKey hexutil.Bytes
}

// storageJSON represents a 256 bit byte array, but allows less than 256 bits when
// unmarshaling from hex.
type storageJSON common.Hash

func (h *storageJSON) UnmarshalText(text []byte) error {
	text = bytes.TrimPrefix(text, []byte("0x"))
	if len(text) > 64 {
		return fmt.Errorf("too many hex characters in storage key/value %q", text)
	}
	offset := len(h) - len(text)/2 // pad on the left
	if _, err := hex.Decode(h[offset:], text); err != nil {
		fmt.Println(err)
		return fmt.Errorf("invalid hex storage key/value %q", text)
	}
	return nil
}

func (h storageJSON) MarshalText() ([]byte, error) {
	return hexutil.Bytes(h[:]).MarshalText()
}

// GenesisMismatchError is raised when trying to overwrite an existing
// genesis block with an incompatible one.
type GenesisMismatchError struct {
	Stored, New common.Hash
}

func (e *GenesisMismatchError) Error() string {
	return fmt.Sprintf("database already contains an incompatible genesis block (have %x, new %x)", e.Stored[:8], e.New[:8])
}

// SetupGenesisBlock writes or updates the genesis block in db.
// The block that will be used is:
//
//                          genesis == nil       genesis != nil
//                       +------------------------------------------
//     db has no genesis |  main-net default  |  genesis
//     db has genesis    |  from DB           |  genesis (if compatible)
//
// The stored chain configuration will be updated if it is compatible (i.e. does not
// specify a fork block below the local head block). In case of a conflict, the
// error is a *params.ConfigCompatError and the new, unwritten config is returned.
//
// The returned chain configuration is never nil.
func SetupGenesisBlock(db abeydb.Database, genesis *Genesis) (*params.ChainConfig, common.Hash, common.Hash, error) {
	if genesis != nil && genesis.Config == nil {
		return params.AllMinervaProtocolChanges, common.Hash{}, common.Hash{}, errGenesisNoConfig
	}

	fastConfig, fastHash, fastErr := setupFastGenesisBlock(db, genesis)
	_, snailHash, _ := setupSnailGenesisBlock(db, genesis)

	return fastConfig, fastHash, snailHash, fastErr

}
func SetupGenesisBlockForLes(db abeydb.Database, genesis *Genesis) (*params.ChainConfig, common.Hash, error) {
	if genesis != nil && genesis.Config == nil {
		return params.AllMinervaProtocolChanges, common.Hash{}, errGenesisNoConfig
	}

	fastConfig, fastHash, fastErr := setupFastGenesisBlockForLes(db, genesis)

	return fastConfig, fastHash, fastErr

}

// setupFastGenesisBlock writes or updates the fast genesis block in db.
// The block that will be used is:
//
//                          genesis == nil       genesis != nil
//                       +------------------------------------------
//     db has no genesis |  main-net default  |  genesis
//     db has genesis    |  from DB           |  genesis (if compatible)
//
// The stored chain configuration will be updated if it is compatible (i.e. does not
// specify a fork block below the local head block). In case of a conflict, the
// error is a *params.ConfigCompatError and the new, unwritten config is returned.
//
// The returned chain configuration is never nil.
func setupFastGenesisBlock(db abeydb.Database, genesis *Genesis) (*params.ChainConfig, common.Hash, error) {
	if genesis != nil && genesis.Config == nil {
		return params.AllMinervaProtocolChanges, common.Hash{}, errGenesisNoConfig
	}

	// Just commit the new block if there is no stored genesis block.
	stored := rawdb.ReadCanonicalHash(db, 0)
	if (stored == common.Hash{}) {
		if genesis == nil {
			log.Info("Writing default main-net genesis block")
			genesis = DefaultGenesisBlock()
		} else {
			log.Info("Writing custom genesis block")
		}
		block, err := genesis.CommitFast(db)
		return genesis.Config, block.Hash(), err
	}

	// Check whether the genesis block is already written.
	if genesis != nil {
		hash := genesis.ToFastBlock(nil).Hash()
		if hash != stored {
			return genesis.Config, hash, &GenesisMismatchError{stored, hash}
		}
	}

	// Get the existing chain configuration.
	newcfg := genesis.configOrDefault(stored)
	storedcfg := rawdb.ReadChainConfig(db, stored)
	if storedcfg == nil {
		log.Warn("Found genesis block without chain config")
		rawdb.WriteChainConfig(db, stored, newcfg)
		return newcfg, stored, nil
	}
	// Special case: don't change the existing config of a non-mainnet chain if no new
	// config is supplied. These chains would get AllProtocolChanges (and a compat error)
	// if we just continued here.
	if genesis == nil && stored != params.MainnetGenesisHash {
		return storedcfg, stored, nil
	}

	// Check config compatibility and write the config. Compatibility errors
	// are returned to the caller unless we're already at block zero.
	height := rawdb.ReadHeaderNumber(db, rawdb.ReadHeadHeaderHash(db))
	if height == nil {
		return newcfg, stored, fmt.Errorf("missing block number for head header hash")
	}
	compatErr := storedcfg.CheckCompatible(newcfg, *height)
	if compatErr != nil && *height != 0 && compatErr.RewindTo != 0 {
		return newcfg, stored, compatErr
	}
	rawdb.WriteChainConfig(db, stored, newcfg)
	return newcfg, stored, nil
}
func setupFastGenesisBlockForLes(db abeydb.Database, genesis *Genesis) (*params.ChainConfig, common.Hash, error) {
	if genesis != nil && genesis.Config == nil {
		return params.AllMinervaProtocolChanges, common.Hash{}, errGenesisNoConfig
	}

	// Just commit the new block if there is no stored genesis block.
	stored := rawdb.ReadCanonicalHash(db, params.LesProtocolGenesisBlock)
	if (stored == common.Hash{}) {
		Lesgenesis := DefaultGenesisBlockForLes()
		log.Info("Writing default main-net les genesis block and Writing genesis block")
		block, err := Lesgenesis.CommitFast(db)
		return genesis.Config, block.Hash(), err
	}

	// Check whether the genesis block is already written.
	if genesis != nil {
		hash := genesis.ToFastBlock(nil).Hash()
		if hash != stored {
			return genesis.Config, hash, &GenesisMismatchError{stored, hash}
		}
	}

	// Get the existing chain configuration.
	newcfg := genesis.configOrDefault(stored)
	storedcfg := rawdb.ReadChainConfig(db, stored)
	if storedcfg == nil {
		log.Warn("Found genesis block without chain config")
		rawdb.WriteChainConfig(db, stored, newcfg)
		return newcfg, stored, nil
	}
	// Special case: don't change the existing config of a non-mainnet chain if no new
	// config is supplied. These chains would get AllProtocolChanges (and a compat error)
	// if we just continued here.
	if genesis == nil && stored != params.MainnetGenesisHashForLes {
		return storedcfg, stored, nil
	}
	// remove check config compatibility
	rawdb.WriteChainConfig(db, stored, newcfg)
	return newcfg, stored, nil
}

// CommitFast writes the block and state of a genesis specification to the database.
// The block is committed as the canonical head block.
func (g *Genesis) CommitFast(db abeydb.Database) (*types.Block, error) {
	block := g.ToFastBlock(db)
	if block.Number().Sign() != 0 {
		return nil, fmt.Errorf("can't commit genesis block with number > 0")
	}
	rawdb.WriteBlock(db, block)
	rawdb.WriteReceipts(db, block.Hash(), block.NumberU64(), nil)
	rawdb.WriteCanonicalHash(db, block.Hash(), block.NumberU64())
	rawdb.WriteHeadBlockHash(db, block.Hash())
	rawdb.WriteHeadHeaderHash(db, block.Hash())
	rawdb.WriteStateGcBR(db, block.NumberU64())

	config := g.Config
	if config == nil {
		config = params.AllMinervaProtocolChanges
	}
	rawdb.WriteChainConfig(db, block.Hash(), config)
	return block, nil
}

// ToFastBlock creates the genesis block and writes state of a genesis specification
// to the given database (or discards it if nil).
func (g *Genesis) ToFastBlock(db abeydb.Database) *types.Block {
	if db == nil {
		db = abeydb.NewMemDatabase()
	}
	statedb, _ := state.New(common.Hash{}, state.NewDatabase(db))
	for addr, account := range g.Alloc {
		statedb.AddBalance(addr, account.Balance)
		statedb.SetCode(addr, account.Code)
		statedb.SetNonce(addr, account.Nonce)
		for key, value := range account.Storage {
			statedb.SetState(addr, key, value)
		}
	}
	consensus.OnceInitImpawnState(g.Config, statedb, new(big.Int).SetUint64(g.Number))
	if consensus.IsTIP8(new(big.Int).SetUint64(g.Number), g.Config, nil) {
		impl := vm.NewImpawnImpl()
		hh := g.Number
		if hh != 0 {
			hh = hh - 1
		}

		for _, member := range g.Committee {
			var err error
			amount := big.NewInt(0)
			if g.Config.ChainID.Uint64() == 179 {
				// mainnet
				amount = new(big.Int).Set(baseAllocamount)
			} else {
				amount = new(big.Int).Set(params.ElectionMinLimitForStaking)
			}
			err = impl.InsertSAccount2(hh, 0, member.Coinbase, member.Publickey, amount, big.NewInt(100), true)
			if err != nil {
				log.Error("ToFastBlock InsertSAccount", "error", err)
			} else {
				vm.GenesisAddLockedBalance(statedb, member.Coinbase, amount)
			}
		}
		_, err := impl.DoElections(1, 0)
		if err != nil {
			log.Error("ToFastBlock DoElections", "error", err)
		}
		err = impl.Shift(1, 0)
		if err != nil {
			log.Error("ToFastBlock Shift", "error", err)
		}
		err = impl.Save(statedb, types.StakingAddress)
		if err != nil {
			log.Error("ToFastBlock IMPL Save", "error", err)
		}
	}

	root := statedb.IntermediateRoot(false)

	head := &types.Header{
		Number:     new(big.Int).SetUint64(g.Number),
		Time:       new(big.Int).SetUint64(g.Timestamp),
		ParentHash: g.ParentHash,
		Extra:      g.ExtraData,
		GasLimit:   g.GasLimit,
		GasUsed:    g.GasUsed,
		Root:       root,
	}
	if g.GasLimit == 0 {
		head.GasLimit = params.GenesisGasLimit
	}
	statedb.Commit(false)
	statedb.Database().TrieDB().Commit(root, true)

	// All genesis committee members are included in switchinfo of block #0
	committee := &types.SwitchInfos{CID: common.Big0, Members: g.Committee, BackMembers: make([]*types.CommitteeMember, 0), Vals: make([]*types.SwitchEnter, 0)}
	for _, member := range committee.Members {
		pubkey, _ := crypto.UnmarshalPubkey(member.Publickey)
		member.Flag = types.StateUsedFlag
		member.MType = types.TypeFixed
		member.CommitteeBase = crypto.PubkeyToAddress(*pubkey)
	}
	return types.NewBlock(head, nil, nil, nil, committee.Members)
}

// MustFastCommit writes the genesis block and state to db, panicking on error.
// The block is committed as the canonical head block.
func (g *Genesis) MustFastCommit(db abeydb.Database) *types.Block {
	block, err := g.CommitFast(db)
	if err != nil {
		panic(err)
	}
	return block
}

// setupSnailGenesisBlock writes or updates the genesis snail block in db.
// The block that will be used is:
//
//                          genesis == nil       genesis != nil
//                       +------------------------------------------
//     db has no genesis |  main-net default  |  genesis
//     db has genesis    |  from DB           |  genesis (if compatible)
//
// The stored chain configuration will be updated if it is compatible (i.e. does not
// specify a fork block below the local head block). In case of a conflict, the
// error is a *params.ConfigCompatError and the new, unwritten config is returned.
//
// The returned chain configuration is never nil.
func setupSnailGenesisBlock(db abeydb.Database, genesis *Genesis) (*params.ChainConfig, common.Hash, error) {
	if genesis != nil && genesis.Config == nil {
		return params.AllMinervaProtocolChanges, common.Hash{}, errGenesisNoConfig
	}
	// Just commit the new block if there is no stored genesis block.
	stored := snaildb.ReadCanonicalHash(db, 0)
	if (stored == common.Hash{}) {
		if genesis == nil {
			log.Info("Writing default main-net genesis block")
			genesis = DefaultGenesisBlock()
		} else {
			log.Info("Writing custom genesis block")
		}
		block, err := genesis.CommitSnail(db)
		return genesis.Config, block.Hash(), err
	}

	// Check whether the genesis block is already written.
	if genesis != nil {
		hash := genesis.ToSnailBlock(nil).Hash()
		if hash != stored {
			return genesis.Config, hash, &GenesisMismatchError{stored, hash}
		}
	}

	// Get the existing chain configuration.
	newcfg := genesis.configOrDefault(stored)
	return newcfg, stored, nil
}

// ToSnailBlock creates the genesis block and writes state of a genesis specification
// to the given database (or discards it if nil).
func (g *Genesis) ToSnailBlock(db abeydb.Database) *types.SnailBlock {
	if db == nil {
		db = abeydb.NewMemDatabase()
	}

	head := &types.SnailHeader{
		Number:     new(big.Int).SetUint64(g.Number),
		Nonce:      types.EncodeNonce(g.Nonce),
		Time:       new(big.Int).SetUint64(g.Timestamp),
		ParentHash: g.ParentHash,
		Extra:      g.ExtraData,
		Difficulty: g.Difficulty,
		MixDigest:  g.Mixhash,
		Coinbase:   g.Coinbase,
	}

	if g.Difficulty == nil {
		head.Difficulty = params.GenesisDifficulty
		g.Difficulty = params.GenesisDifficulty
	}

	fastBlock := g.ToFastBlock(db)
	fruitHead := &types.SnailHeader{
		Number:          new(big.Int).SetUint64(g.Number),
		Nonce:           types.EncodeNonce(g.Nonce),
		Time:            new(big.Int).SetUint64(g.Timestamp),
		ParentHash:      g.ParentHash,
		FastNumber:      fastBlock.Number(),
		FastHash:        fastBlock.Hash(),
		FruitDifficulty: new(big.Int).Div(g.Difficulty, params.FruitBlockRatio),
		Coinbase:        g.Coinbase,
	}
	fruit := types.NewSnailBlock(fruitHead, nil, nil, nil, g.Config)

	return types.NewSnailBlock(head, []*types.SnailBlock{fruit}, nil, nil, g.Config)
}

// CommitSnail writes the block and state of a genesis specification to the database.
// The block is committed as the canonical head block.
func (g *Genesis) CommitSnail(db abeydb.Database) (*types.SnailBlock, error) {
	block := g.ToSnailBlock(db)
	if block.Number().Sign() != 0 {
		return nil, fmt.Errorf("can't commit genesis block with number > 0")
	}
	snaildb.WriteTd(db, block.Hash(), block.NumberU64(), g.Difficulty)
	snaildb.WriteBlock(db, block)
	snaildb.WriteFtLookupEntries(db, block)
	snaildb.WriteCanonicalHash(db, block.Hash(), block.NumberU64())
	snaildb.WriteHeadBlockHash(db, block.Hash())
	snaildb.WriteHeadHeaderHash(db, block.Hash())

	// config := g.Config
	// if config == nil {
	// 	config = params.AllMinervaProtocolChanges
	// }
	// snaildb.WriteChainConfig(db, block.Hash(), config)
	return block, nil
}

// MustSnailCommit writes the genesis block and state to db, panicking on error.
// The block is committed as the canonical head block.
func (g *Genesis) MustSnailCommit(db abeydb.Database) *types.SnailBlock {
	block, err := g.CommitSnail(db)
	if err != nil {
		panic(err)
	}
	return block
}

// DefaultGenesisBlock returns the Abeychain main net snail block.
func DefaultGenesisBlock() *Genesis {
	allocAmount := new(big.Int).Mul(big.NewInt(990000000), big.NewInt(1e18))
	key1 := hexutil.MustDecode("0x04e9dd750f5a409ae52533241c0b4a844c000613f34320c737f787b69ebaca45f10703f77a1b78ed00a8bd5c0bc22508262a33a81e65b2e90a4eb9a8f5a6391db3")
	key2 := hexutil.MustDecode("0x04c042a428a7df304ac7ea81c1555da49310cebb079a905c8256080e8234af804dad4ad9995771f96fba8182b117f62d2f1a6643e27f5f272c293a8301b6a84442")
	key3 := hexutil.MustDecode("0x04dc1da011509b6ea17527550cc480f6eb076a225da2bcc87ec7a24669375f229945d76e4f9dbb4bd26c72392050a18c3922bd7ef38c04e018192b253ef4fc9dcb")
	key4 := hexutil.MustDecode("0x04952af3d04c0b0ba3d16eea8ca0ab6529f5c6e2d08f4aa954ae2296d4ded9f04c8a9e1d52be72e6cebb86b4524645fafac04ac8633c4b33638254b2eb64a89c6a")
	key5 := hexutil.MustDecode("0x04290cdc7fe53df0f93d43264302337751a58bcf67ee56799abea93b0a6205be8b3c8f1c9dac281f4d759475076596d30aa360d0c3b160dc28ea300b7e4925fb32")
	key6 := hexutil.MustDecode("0x04427e32084f7565970d74a3df317b68de59e62f28b86700c8a5e3ae83a781ec163c4c83544bd8f88b8d70c4d71f2827b7b279bfc25481453dd35533cf234b2dfe")
	key7 := hexutil.MustDecode("0x04dd9980aac0edead2de77cc6cde74875c14ac21d95a1cb49d36b810246b50420f1dc7c19f5296d739fcfceb454a18f250fa7802280f5298e5e2b2a591faa15cf9")
	key8 := hexutil.MustDecode("0x04039dd0fb3869e7d2a1eeb95c9a6475771883614b289c604bf6fef2e1e9dd57340d888f59db0129d250394909d4a3b041bd66e6b83f345b38a397fdeb036b3e1c")
	key9 := hexutil.MustDecode("0x042ec25823b375f655117d1a7003f9526e9adc0d6d50150812e0408fbfb3256810c912d7cd7e5441bc5e54ac143fb6274ac496548e1a2aaaf370e8aa8b5b1ced4d")
	key10 := hexutil.MustDecode("0x043e3014c29e42015fe891ca3e97e5fb05961beca9e349b821c6738eadd17d9b784295638e26c1d7ca71beb8703ec8cf944c67f3835bf5119f78192b535ac6a5e0")

	return &Genesis{
		Config:     params.MainnetChainConfig,
		Nonce:      402,
		ExtraData:  hexutil.MustDecode("0x0123456789"),
		GasLimit:   16777216,
		Difficulty: big.NewInt(8388608),
		//Timestamp:  1553918400,
		Coinbase:   common.HexToAddress("0x0000000000000000000000000000000000000000"),
		Mixhash:    common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		ParentHash: common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		//Alloc:      decodePrealloc(mainnetAllocData),
		Alloc: map[common.Address]types.GenesisAccount{
			common.HexToAddress("0x80f0a40f60f08a4D7345A8411FF1721E25d23DF5"): {Balance: baseAllocamount},
			common.HexToAddress("0x1Cfe2A1D7B9CBfce14d06bAFfa338b2465216255"): {Balance: baseAllocamount},
			common.HexToAddress("0x1275db492b0d02855a38Bd3Cdf73C92137CD1691"): {Balance: baseAllocamount},
			common.HexToAddress("0xF11A544F74a2F4Faa2AF8Aa38F9388A4Cc2F3ACC"): {Balance: baseAllocamount},
			common.HexToAddress("0xc30E75016F5a82EE6f0A7989F9DCD5F030c83B3A"): {Balance: baseAllocamount},
			common.HexToAddress("0x1e2E48Fa3cC3417474EC264DE53D6305109af1b9"): {Balance: baseAllocamount},
			common.HexToAddress("0x7AdC129C637f93C9392c59e9C4d406FDC28aAB43"): {Balance: baseAllocamount},
			common.HexToAddress("0xf9621AEa3d6492d43dC96b5472C4680021793109"): {Balance: baseAllocamount},
			common.HexToAddress("0x5552FAC84cD38DEdAf8c80a195591CBCED1f4A8D"): {Balance: baseAllocamount},
			common.HexToAddress("0xBa9779b7173099354630BD87b5b972441E3605bd"): {Balance: baseAllocamount},
			// 9.9
			common.HexToAddress("0xEc1F80E553Bf43229EBA70d254E09DD188D604f2"): {Balance: allocAmount},
		},
		Committee: []*types.CommitteeMember{
			&types.CommitteeMember{Coinbase: common.HexToAddress("0x80f0a40f60f08a4D7345A8411FF1721E25d23DF5"), Publickey: key1},
			&types.CommitteeMember{Coinbase: common.HexToAddress("0x1Cfe2A1D7B9CBfce14d06bAFfa338b2465216255"), Publickey: key2},
			&types.CommitteeMember{Coinbase: common.HexToAddress("0x1275db492b0d02855a38Bd3Cdf73C92137CD1691"), Publickey: key3},
			&types.CommitteeMember{Coinbase: common.HexToAddress("0xF11A544F74a2F4Faa2AF8Aa38F9388A4Cc2F3ACC"), Publickey: key4},
			&types.CommitteeMember{Coinbase: common.HexToAddress("0xc30E75016F5a82EE6f0A7989F9DCD5F030c83B3A"), Publickey: key5},
			&types.CommitteeMember{Coinbase: common.HexToAddress("0x1e2E48Fa3cC3417474EC264DE53D6305109af1b9"), Publickey: key6},
			&types.CommitteeMember{Coinbase: common.HexToAddress("0x7AdC129C637f93C9392c59e9C4d406FDC28aAB43"), Publickey: key7},
			&types.CommitteeMember{Coinbase: common.HexToAddress("0xf9621AEa3d6492d43dC96b5472C4680021793109"), Publickey: key8},
			&types.CommitteeMember{Coinbase: common.HexToAddress("0x5552FAC84cD38DEdAf8c80a195591CBCED1f4A8D"), Publickey: key9},
			&types.CommitteeMember{Coinbase: common.HexToAddress("0xBa9779b7173099354630BD87b5b972441E3605bd"), Publickey: key10},
		},
	}
}

func (g *Genesis) configOrDefault(ghash common.Hash) *params.ChainConfig {
	switch {
	case g != nil:
		return g.Config
	case ghash == params.MainnetGenesisHash:
		return params.MainnetChainConfig
	case ghash == params.MainnetSnailGenesisHash:
		return params.MainnetChainConfig
	case ghash == params.TestnetGenesisHash:
		return params.TestnetChainConfig
	case ghash == params.TestnetSnailGenesisHash:
		return params.TestnetChainConfig
	default:
		return params.AllMinervaProtocolChanges
	}
}

func decodePrealloc(data string) types.GenesisAlloc {
	var p []struct{ Addr, Balance *big.Int }
	if err := rlp.NewStream(strings.NewReader(data), 0).Decode(&p); err != nil {
		panic(err)
	}
	ga := make(types.GenesisAlloc, len(p))
	for _, account := range p {
		ga[common.BigToAddress(account.Addr)] = types.GenesisAccount{Balance: account.Balance}
	}
	return ga
}

// GenesisFastBlockForTesting creates and writes a block in which addr has the given wei balance.
func GenesisFastBlockForTesting(db abeydb.Database, addr common.Address, balance *big.Int) *types.Block {
	g := Genesis{Alloc: types.GenesisAlloc{addr: {Balance: balance}}, Config: params.AllMinervaProtocolChanges}
	return g.MustFastCommit(db)
}

// GenesisSnailBlockForTesting creates and writes a block in which addr has the given wei balance.
func GenesisSnailBlockForTesting(db abeydb.Database, addr common.Address, balance *big.Int) *types.SnailBlock {
	g := Genesis{Alloc: types.GenesisAlloc{addr: {Balance: balance}}, Config: params.AllMinervaProtocolChanges}
	return g.MustSnailCommit(db)
}

// DefaultDevGenesisBlock returns the Rinkeby network genesis block.
func DefaultDevGenesisBlock() *Genesis {
	i, _ := new(big.Int).SetString("90000000000000000000000", 10)
	// priv1: 55dcdfd62f565a66e1886959e82a365e4987ed0b405adc43614a42c3481edd1a
	// addr1: 0x3e3429F72450A39CE227026E8DdeF331E9973E4d
	key1 := hexutil.MustDecode("0x04600254af4ce74276f54b4f9df193f2cb72ed76b7341cb144f4d6f1408402dc10719eebdcb947ced9ac6fe9a690e004692db6222de7867cbab712246eb23a50b7")
	// priv2: a0eb966cae593e0d85c7eda4ad4815d0c857bee9a7085a8b19e52e3227138ae4
	// addr2: 0xf353ab1417177F766497bF716D7aAd4ECd5f36C8
	key2 := hexutil.MustDecode("0x043ae657860b05d119351eac9d2f4531811ade3895ee2df00661368ca528ee36ceb850315f7bb566c6bbebf765e2c15f6af16b253a4d3d930cca7a191ae14af80d")
	// priv3: 5b743d4234c54710a644ff93a6f5284af065d2a42fff5b51de73a7c13d427b1c
	// addr3: 0x8fF345746C3d3435a105538E4c024Af5FE700598
	key3 := hexutil.MustDecode("0x049e0a67955d69e28faabe654b4a8f85e7d32b32fd2687a080e6357b53ec9413ad4f472d979bdccfe21cb135c7e144ca90f2beeb728b06e59f80918c7e52fbc6ff")
	// priv4: 229ca04fb83ec698296037c7d2b04a731905df53b96c260555cbeed9e4c64036
	// addr4: 0xf0C8898B2016Afa0Ec5912413ebe403930446779
	key4 := hexutil.MustDecode("0x04718502f879a949ca5fa29f78f1d3cef362ecdc36ee42a3023cca80371c2e1936d1f632a0ec5bf5edb2af228a5ba1669d31ea55df87548de172e5767b9201097d")

	return &Genesis{
		Config:     params.DevnetChainConfig,
		Nonce:      928,
		ExtraData:  nil,
		GasLimit:   88080384,
		Difficulty: big.NewInt(20000),
		//Alloc:      decodePrealloc(mainnetAllocData),
		Alloc: map[common.Address]types.GenesisAccount{
			common.HexToAddress("0x3e3429F72450A39CE227026E8DdeF331E9973E4d"): {Balance: i},
			common.HexToAddress("0xf353ab1417177F766497bF716D7aAd4ECd5f36C8"): {Balance: i},
			common.HexToAddress("0x8fF345746C3d3435a105538E4c024Af5FE700598"): {Balance: i},
			common.HexToAddress("0xf0C8898B2016Afa0Ec5912413ebe403930446779"): {Balance: i},
		},
		Committee: []*types.CommitteeMember{
			{Coinbase: common.HexToAddress("0x3e3429F72450A39CE227026E8DdeF331E9973E4d"), Publickey: key1},
			{Coinbase: common.HexToAddress("0xf353ab1417177F766497bF716D7aAd4ECd5f36C8"), Publickey: key2},
			{Coinbase: common.HexToAddress("0x8fF345746C3d3435a105538E4c024Af5FE700598"), Publickey: key3},
			{Coinbase: common.HexToAddress("0xf0C8898B2016Afa0Ec5912413ebe403930446779"), Publickey: key4},
		},
	}
}

func DefaultSingleNodeGenesisBlock() *Genesis {
	i, _ := new(big.Int).SetString("90000000000000000000000", 10)
	// priv: 229ca04fb83ec698296037c7d2b04a731905df53b96c260555cbeed9e4c64036
	key1 := hexutil.MustDecode("0x04718502f879a949ca5fa29f78f1d3cef362ecdc36ee42a3023cca80371c2e1936d1f632a0ec5bf5edb2af228a5ba1669d31ea55df87548de172e5767b9201097d")

	return &Genesis{
		Config:     params.SingleNodeChainConfig,
		Nonce:      66,
		ExtraData:  nil,
		GasLimit:   22020096,
		Difficulty: big.NewInt(256),
		//Alloc:      decodePrealloc(mainnetAllocData),
		Alloc: map[common.Address]types.GenesisAccount{
			common.HexToAddress("0xf0C8898B2016Afa0Ec5912413ebe403930446779"): {Balance: i},
		},
		Committee: []*types.CommitteeMember{
			{Coinbase: common.HexToAddress("0xf0C8898B2016Afa0Ec5912413ebe403930446779"), Publickey: key1},
		},
	}
}

// DefaultTestnetGenesisBlock returns the Ropsten network genesis block.
func DefaultTestnetGenesisBlock() *Genesis {
	// priv1: 55dcdfd62f565a66e1886959e82a365e4987ed0b405adc43614a42c3481edd1a
	seedkey1 := hexutil.MustDecode("0x04600254af4ce74276f54b4f9df193f2cb72ed76b7341cb144f4d6f1408402dc10719eebdcb947ced9ac6fe9a690e004692db6222de7867cbab712246eb23a50b7")
	// priv2: a0eb966cae593e0d85c7eda4ad4815d0c857bee9a7085a8b19e52e3227138ae4
	seedkey2 := hexutil.MustDecode("0x043ae657860b05d119351eac9d2f4531811ade3895ee2df00661368ca528ee36ceb850315f7bb566c6bbebf765e2c15f6af16b253a4d3d930cca7a191ae14af80d")
	// priv3: 5b743d4234c54710a644ff93a6f5284af065d2a42fff5b51de73a7c13d427b1c
	seedkey3 := hexutil.MustDecode("0x049e0a67955d69e28faabe654b4a8f85e7d32b32fd2687a080e6357b53ec9413ad4f472d979bdccfe21cb135c7e144ca90f2beeb728b06e59f80918c7e52fbc6ff")
	// priv4: 229ca04fb83ec698296037c7d2b04a731905df53b96c260555cbeed9e4c64036
	seedkey4 := hexutil.MustDecode("0x04718502f879a949ca5fa29f78f1d3cef362ecdc36ee42a3023cca80371c2e1936d1f632a0ec5bf5edb2af228a5ba1669d31ea55df87548de172e5767b9201097d")
	// priv:  e162820ca35b8753b0495243fb5e54ed47d2f53319a149d7750da2ccb135d249
	// addr: 0x37C229201a1d05b7326a2A8c64D8c7966F795a3B
	// seed4
	//coinbase := common.HexToAddress("0xf0C8898B2016Afa0Ec5912413ebe403930446779")
	amount1 := new(big.Int).Mul(big.NewInt(900000000000000000), big.NewInt(1e18))
	return &Genesis{
		Config:     params.TestnetChainConfig,
		Nonce:      0,
		ExtraData:  hexutil.MustDecode("0x54727565436861696E20546573744E6574203035"),
		GasLimit:   20971520,
		Difficulty: big.NewInt(100000),
		Timestamp:  1537891200,
		Coinbase:   common.HexToAddress("0x0000000000000000000000000000000000000000"),
		Mixhash:    common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		ParentHash: common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Alloc: map[common.Address]types.GenesisAccount{
			common.HexToAddress("0x37C229201a1d05b7326a2A8c64D8c7966F795a3B"): {Balance: amount1},
			common.HexToAddress("0x3e3429F72450A39CE227026E8DdeF331E9973E4d"): {Balance: amount1},
			common.HexToAddress("0xf353ab1417177F766497bF716D7aAd4ECd5f36C8"): {Balance: amount1},
			common.HexToAddress("0x8fF345746C3d3435a105538E4c024Af5FE700598"): {Balance: amount1},
			common.HexToAddress("0xf0C8898B2016Afa0Ec5912413ebe403930446779"): {Balance: amount1},
		},
		Committee: []*types.CommitteeMember{
			&types.CommitteeMember{Coinbase: common.HexToAddress("0x3e3429F72450A39CE227026E8DdeF331E9973E4d"), Publickey: seedkey1},
			&types.CommitteeMember{Coinbase: common.HexToAddress("0xf353ab1417177F766497bF716D7aAd4ECd5f36C8"), Publickey: seedkey2},
			&types.CommitteeMember{Coinbase: common.HexToAddress("0x8fF345746C3d3435a105538E4c024Af5FE700598"), Publickey: seedkey3},
			&types.CommitteeMember{Coinbase: common.HexToAddress("0xf0C8898B2016Afa0Ec5912413ebe403930446779"), Publickey: seedkey4},
		},
	}
}
func DefaultGenesisBlockForLes() *LesGenesis {
	key1 := hexutil.MustDecode("0x04600254af4ce74276f54b4f9df193f2cb72ed76b7341cb144f4d6f1408402dc10719eebdcb947ced9ac6fe9a690e004692db6222de7867cbab712246eb23a50b7")
	// priv2: a0eb966cae593e0d85c7eda4ad4815d0c857bee9a7085a8b19e52e3227138ae4
	// addr2: 0xf353ab1417177F766497bF716D7aAd4ECd5f36C8
	key2 := hexutil.MustDecode("0x043ae657860b05d119351eac9d2f4531811ade3895ee2df00661368ca528ee36ceb850315f7bb566c6bbebf765e2c15f6af16b253a4d3d930cca7a191ae14af80d")
	// priv3: 5b743d4234c54710a644ff93a6f5284af065d2a42fff5b51de73a7c13d427b1c
	// addr3: 0x8fF345746C3d3435a105538E4c024Af5FE700598
	key3 := hexutil.MustDecode("0x049e0a67955d69e28faabe654b4a8f85e7d32b32fd2687a080e6357b53ec9413ad4f472d979bdccfe21cb135c7e144ca90f2beeb728b06e59f80918c7e52fbc6ff")
	// priv4: 229ca04fb83ec698296037c7d2b04a731905df53b96c260555cbeed9e4c64036
	// addr4: 0xf0C8898B2016Afa0Ec5912413ebe403930446779
	key4 := hexutil.MustDecode("0x04718502f879a949ca5fa29f78f1d3cef362ecdc36ee42a3023cca80371c2e1936d1f632a0ec5bf5edb2af228a5ba1669d31ea55df87548de172e5767b9201097d")

	logs := common.FromHex("0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")
	return &LesGenesis{
		Header: &types.Header{
			ParentHash:    common.HexToHash("0xf741dc3d4861af7d5ebb6d2fb70da444027f6345bdfecc1d27fbd71839dd52b4"),
			Root:          common.HexToHash("0xc6d054d6132d77257344a97dcc100ef645fb55840e787af46d96ccb0df5b404c"),
			TxHash:        common.HexToHash("0x16645d96c08755c115738139ff9d84002f04fb076b1d1384c063cdc83cb67e32"),
			ReceiptHash:   common.HexToHash("0xd95b673818fa493deec414e01e610d97ee287c9421c8eff4102b1647c1a184e4"),
			CommitteeHash: common.HexToHash("0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"),
			Proposer:      common.HexToAddress("0x3dde9f28c3ec9eef3e5bf8b510be506513226e2e"),
			Bloom:         types.BytesToBloom(logs),
			SnailHash:     common.HexToHash("0x7578c65cb797565143b1f84a3c4bbe30687f3200cb279bed59fcde344a1fe4eb"),
			SnailNumber:   big.NewInt(0),
			Number:        big.NewInt(9000000),
			GasLimit:      16000000,
			GasUsed:       42000,
			Time:          big.NewInt(1663377377),
			Extra:         hexutil.MustDecode(""),
		},
		Committee: []*types.CommitteeMember{
			{Coinbase: common.HexToAddress("0x3e3429F72450A39CE227026E8DdeF331E9973E4d"), Publickey: key1},
			{Coinbase: common.HexToAddress("0xf353ab1417177F766497bF716D7aAd4ECd5f36C8"), Publickey: key2},
			{Coinbase: common.HexToAddress("0x8fF345746C3d3435a105538E4c024Af5FE700598"), Publickey: key3},
			{Coinbase: common.HexToAddress("0xf0C8898B2016Afa0Ec5912413ebe403930446779"), Publickey: key4},
		},
	}
}
func (g *LesGenesis) ToLesFastBlock() *types.Block {
	head := g.Header
	// All genesis committee members are included in switchinfo of block #0
	committee := &types.SwitchInfos{CID: common.Big0, Members: g.Committee, BackMembers: make([]*types.CommitteeMember, 0), Vals: make([]*types.SwitchEnter, 0)}
	for _, member := range committee.Members {
		pubkey, _ := crypto.UnmarshalPubkey(member.Publickey)
		member.Flag = types.StateUsedFlag
		member.MType = types.TypeFixed
		member.CommitteeBase = crypto.PubkeyToAddress(*pubkey)
	}
	return types.NewBlock(head, nil, nil, nil, committee.Members)
}
func (g *LesGenesis) CommitFast(db abeydb.Database) (*types.Block, error) {
	block := g.ToLesFastBlock()
	if block.Number().Sign() != 0 {
		return nil, fmt.Errorf("can't commit genesis block with number > 0")
	}
	rawdb.WriteBlock(db, block)
	rawdb.WriteReceipts(db, block.Hash(), block.NumberU64(), nil)
	rawdb.WriteCanonicalHash(db, block.Hash(), block.NumberU64())
	rawdb.WriteHeadBlockHash(db, block.Hash())
	rawdb.WriteHeadHeaderHash(db, block.Hash())
	rawdb.WriteStateGcBR(db, block.NumberU64())

	config := g.Config
	if config == nil {
		config = params.AllMinervaProtocolChanges
	}
	rawdb.WriteChainConfig(db, block.Hash(), config)
	return block, nil
}
