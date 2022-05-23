package main

import (
	"crypto/ecdsa"
	"fmt"
	"github.com/abeychain/go-abey/accounts/abi"
	"github.com/abeychain/go-abey/accounts/abi/bind"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/abeychain/go-abey/abeydb"
	"github.com/abeychain/go-abey/common"
	"github.com/abeychain/go-abey/common/hexutil"
	"github.com/abeychain/go-abey/consensus/minerva"
	"github.com/abeychain/go-abey/core"
	"github.com/abeychain/go-abey/core/types"
	"github.com/abeychain/go-abey/core/vm"
	"github.com/abeychain/go-abey/crypto"
	"github.com/abeychain/go-abey/log"
	"github.com/abeychain/go-abey/params"
)

func init() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(false))))
}

func setHighLevelForLog() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StreamHandler(os.Stderr, log.TerminalFormat(false))))
}
func DefaulGenesisBlock() *core.Genesis {
	i, _ := new(big.Int).SetString("90000000000000000000000000", 10)
	key1 := hexutil.MustDecode("0x04d341c94a16b02cee86a627d0f6bc6e814741af4cab5065637aa013c9a7d9f26051bb6546030cd67e440d6df741cb65debaaba6c0835579f88a282193795ed369")
	key2 := hexutil.MustDecode("0x0496e0f18d4bf38e0b0de161edd2aa168adaf6842706e5ebf31e1d46cb79fe7b720c750a9e7a3e1a528482b0da723b5dfae739379e555a2893e8693747559f83cd")
	key3 := hexutil.MustDecode("0x0418196ee090081bdec01e8840941b9f6a141a713dd3461b78825edf0d8a7f8cdf3f612832dc9d94249c10c72629ea59fbe0bdd09bea872ddab2799748964c93a8")
	key4 := hexutil.MustDecode("0x04c4935993a3ce206318ab884871fbe2d4dce32a022795c674784f58e7faf3239631b6952b82471fe1e93ef999108a18d028e5d456cd88bb367d610c5e57c7e443")

	return &core.Genesis{
		Config:     params.DevnetChainConfig,
		Nonce:      928,
		ExtraData:  nil,
		GasLimit:   88080384000,
		Difficulty: big.NewInt(20000),
		Alloc: map[common.Address]types.GenesisAccount{
			common.HexToAddress("0xC02f50f4F41f46b6a2f08036ae65039b2F9aCd69"): {Balance: i},
			common.HexToAddress("0x6d348e0188Cc2596aaa4046a1D50bB3BA50E8524"): {Balance: i},
			common.HexToAddress("0xE803895897C3cCd35315b2E41c95F817543811A5"): {Balance: i},
			common.HexToAddress("0x3F739ffD8A59965E07e1B8d7CCa938125BCe8CFb"): {Balance: i},
		},
		Committee: []*types.CommitteeMember{
			{Coinbase: common.HexToAddress("0x3f9061bf173d8f096c94db95c40f3658b4c7eaad"), Publickey: key1},
			{Coinbase: common.HexToAddress("0x2cdac3658f85b5da3b70223cc3ad3b2dfe7c1930"), Publickey: key2},
			{Coinbase: common.HexToAddress("0x41acde8dd7611338c2a30e90149e682566716e9d"), Publickey: key3},
			{Coinbase: common.HexToAddress("0x0ffd116a3bf97a7112ff8779cc770b13ea3c66a5"), Publickey: key4},
		},
	}
}
func DefaulGenesisBlock2(allocAdrress []common.Address) *core.Genesis {
	i, _ := new(big.Int).SetString("90000000000000000000000000", 10)
	key1 := hexutil.MustDecode("0x04d341c94a16b02cee86a627d0f6bc6e814741af4cab5065637aa013c9a7d9f26051bb6546030cd67e440d6df741cb65debaaba6c0835579f88a282193795ed369")
	key2 := hexutil.MustDecode("0x0496e0f18d4bf38e0b0de161edd2aa168adaf6842706e5ebf31e1d46cb79fe7b720c750a9e7a3e1a528482b0da723b5dfae739379e555a2893e8693747559f83cd")
	key3 := hexutil.MustDecode("0x0418196ee090081bdec01e8840941b9f6a141a713dd3461b78825edf0d8a7f8cdf3f612832dc9d94249c10c72629ea59fbe0bdd09bea872ddab2799748964c93a8")
	key4 := hexutil.MustDecode("0x04c4935993a3ce206318ab884871fbe2d4dce32a022795c674784f58e7faf3239631b6952b82471fe1e93ef999108a18d028e5d456cd88bb367d610c5e57c7e443")

	alloc := make(map[common.Address]types.GenesisAccount)
	for _,addr := range allocAdrress {
		alloc[addr] = types.GenesisAccount{Balance: i}
	}
	return &core.Genesis{
		Config:     params.DevnetChainConfig,
		Nonce:      928,
		ExtraData:  nil,
		GasLimit:   88080384000,
		Difficulty: big.NewInt(1000),
		Alloc: 		alloc,
		Committee: []*types.CommitteeMember{
			{Coinbase: common.HexToAddress("0x3f9061bf173d8f096c94db95c40f3658b4c7eaad"), Publickey: key1},
			{Coinbase: common.HexToAddress("0x2cdac3658f85b5da3b70223cc3ad3b2dfe7c1930"), Publickey: key2},
			{Coinbase: common.HexToAddress("0x41acde8dd7611338c2a30e90149e682566716e9d"), Publickey: key3},
			{Coinbase: common.HexToAddress("0x0ffd116a3bf97a7112ff8779cc770b13ea3c66a5"), Publickey: key4},
		},
	}
}

var (
	engine    = minerva.NewFaker()
	db        = abeydb.NewMemDatabase()
	gspec     = DefaulGenesisBlock()
	signer    = types.NewTIP1Signer(gspec.Config.ChainID)
	priKey, _ = crypto.HexToECDSA("0260c952edc49037129d8cabbe4603d15185d83aa718291279937fb6db0fa7a2")
	mAccount  = common.HexToAddress("0xC02f50f4F41f46b6a2f08036ae65039b2F9aCd69")
)

///////////////////////////////////////////////////////////////////////
func TestParallelTX(t *testing.T) {
	params.MinTimeGap = big.NewInt(0)
	params.SnailRewardInterval = big.NewInt(3)
	sendNumber := 4000
	delegateKey := make([]*ecdsa.PrivateKey, sendNumber)
	delegateAddr := make([]common.Address, sendNumber)
	for i := 0; i < sendNumber; i++ {
		delegateKey[i], _ = crypto.GenerateKey()
		delegateAddr[i] = crypto.PubkeyToAddress(delegateKey[i].PublicKey)
	}
	genesis := gspec.MustFastCommit(db)
	chain, _ := core.GenerateChain(gspec.Config, genesis, engine, db, 10, func(i int, gen *core.BlockGen) {
		switch i {
		case 0:
			// In block 1, addr1 sends addr2 some ether.
			fmt.Println("balance ", weiToAbey(gen.GetState().GetBalance(mAccount)))
			nonce := gen.TxNonce(mAccount)
			for _, v := range delegateAddr {
				tx, _ := types.SignTx(types.NewTransaction(nonce, v, abeyToWei(2), params.TxGas, nil, nil), signer, priKey)
				gen.AddTx(tx)
				nonce = nonce + 1
			}
		case 1:
			// In block 2, addr1 sends some more ether to addr2.
			nonce := gen.TxNonce(mAccount)
			key, _ := crypto.GenerateKey()
			coinbase := crypto.PubkeyToAddress(key.PublicKey)
			for i := 0; i < sendNumber; i++ {
				tx, _ := types.SignTx(types.NewTransaction(nonce, coinbase, abeyToWei(2), params.TxGas, nil, nil), signer, priKey)
				gen.AddTx(tx)
				nonce = nonce + 1
			}
		case 4:
			for k, v := range delegateAddr {
				key, _ := crypto.GenerateKey()
				coinbase := crypto.PubkeyToAddress(key.PublicKey)
				nonce := gen.TxNonce(v)
				for i := 0; i < 1; i++ {
					tx, _ := types.SignTx(types.NewTransaction(nonce, coinbase, new(big.Int).SetInt64(30000), params.TxGas, nil, nil), signer, delegateKey[k])
					gen.AddTx(tx)
					nonce = nonce + 1
				}
			}
		}
	})
	//params.ApplytxTime = 0
	//params.FinalizeTime = 0
	//params.ProcessTime = 0
	//params.InsertBlockTime = 0
	repeat := int64(2)
	for i := 0; i < int(repeat); i++ {
		db1 := abeydb.NewMemDatabase()
		gspec.MustFastCommit(db1)

		blockchain, err := core.NewBlockChain(db1, nil, gspec.Config, engine, vm.Config{})
		if err != nil {
			fmt.Println("NewBlockChain ", err)
		}
		if _, err := blockchain.InsertChain(chain); err != nil {
			panic(err)
		}
	}
	//log.Info("Process:",
	//	"applyTxs", common.PrettyDuration(time.Duration(int64(params.ApplytxTime)/repeat)),
	//	"finalize", common.PrettyDuration(time.Duration(int64(params.FinalizeTime)/repeat)),
	//	"Process", common.PrettyDuration(time.Duration(int64(params.ProcessTime)/repeat)),
	//	"insertblock", common.PrettyDuration(time.Duration(int64(params.InsertBlockTime)/repeat)))
}

func Test01(t *testing.T) {
	params.MinTimeGap = big.NewInt(0)
	params.SnailRewardInterval = big.NewInt(3)

	sendNumber := 100
	delegateKey := make([]*ecdsa.PrivateKey, sendNumber)
	delegateAddr := make([]common.Address, sendNumber)
	contracts := make(map[common.Address]common.Address)

	for i := 0; i < sendNumber; i++ {
		delegateKey[i], _ = crypto.GenerateKey()
		delegateAddr[i] = crypto.PubkeyToAddress(delegateKey[i].PublicKey)
	}
	genesis := gspec.MustFastCommit(db)
	chain, _ := core.GenerateChain(gspec.Config, genesis, engine, db, 10, func(i int, gen *core.BlockGen) {
		switch i {
		case 0:
			// In block 1, addr1 sends addr2 some ether.
			fmt.Println("balance ", weiToAbey(gen.GetState().GetBalance(mAccount)))
			nonce := gen.TxNonce(mAccount)
			for _, v := range delegateAddr {
				tx, _ := types.SignTx(types.NewTransaction(nonce, v, abeyToWei(20), params.TxGas, nil, nil), signer, priKey)
				gen.AddTx(tx)
				nonce = nonce + 1
			}
		case 1:
			//In block 2, deploy the contract.
			for i := 0; i < sendNumber; i++ {
				nonce := gen.TxNonce(delegateAddr[i])
				tx, contractAddr := makeContractTransaction(delegateKey[i], nonce, common.FromHex(CoinBin),gspec.Config.ChainID)
				contracts[delegateAddr[i]] = contractAddr

				gen.AddTx(tx)
				fmt.Println("from",delegateAddr[i],"contract address",contractAddr)
			}
		case 2:
			// in block 3, call function for the contract
			parsed, err := abi.JSON(strings.NewReader(CoinABI))
			if err != nil {
				panic(fmt.Sprintf("Failed to parse abi %v", err))
			}
			for i:=0;i<sendNumber;i++ {
				addr1,addr2 := makeAddress(),makeAddress()
				input, _ := parsed.Pack("rechargeToAccount", addr1, addr2)
				value := abeyToWei(2)
				nonce := gen.TxNonce(delegateAddr[i])
				tx := makeCallTransaction(delegateKey[i], contracts[delegateAddr[i]], nonce, input, value,gspec.Config.ChainID)
				gen.AddTx(tx)
			}
		}
	})

	repeat := int64(2)
	for i := 0; i < int(repeat); i++ {
		db1 := abeydb.NewMemDatabase()
		gspec.MustFastCommit(db1)

		blockchain, err := core.NewBlockChain(db1, nil, gspec.Config, engine, vm.Config{})
		if err != nil {
			fmt.Println("NewBlockChain ", err)
		}
		if _, err := blockchain.InsertChain(chain); err != nil {
			panic(err)
		}
	}
}
func TestCmpSerialAndParallelBlock(t *testing.T) {
	params.MinTimeGap = big.NewInt(0)
	params.SnailRewardInterval = big.NewInt(3)

	sendNumber := 500
	delegateKey := make([]*ecdsa.PrivateKey, sendNumber)
	delegateAddr := make([]common.Address, sendNumber)
	contracts := make(map[common.Address]common.Address)

	for i := 0; i < sendNumber; i++ {
		delegateKey[i], _ = crypto.GenerateKey()
		delegateAddr[i] = crypto.PubkeyToAddress(delegateKey[i].PublicKey)
	}
	genesis := gspec.MustFastCommit(db)
	chain, _ := core.GenerateChain(gspec.Config, genesis, engine, db, 10, func(i int, gen *core.BlockGen) {
		switch i {
		case 0:
			// In block 1, addr1 sends addr2 some ether.
			fmt.Println("balance ", weiToAbey(gen.GetState().GetBalance(mAccount)))
			nonce := gen.TxNonce(mAccount)
			for _, v := range delegateAddr {
				tx, _ := types.SignTx(types.NewTransaction(nonce, v, abeyToWei(20), params.TxGas, nil, nil), signer, priKey)
				gen.AddTx(tx)
				nonce = nonce + 1
			}
		case 1:
			//In block 2, deploy the contract.
			for i := 0; i < sendNumber; i++ {
				nonce := gen.TxNonce(delegateAddr[i])
				tx, contractAddr := makeContractTransaction(delegateKey[i], nonce, common.FromHex(CoinBin),gspec.Config.ChainID)
				contracts[delegateAddr[i]] = contractAddr

				gen.AddTx(tx)
				//fmt.Println("from",delegateAddr[i],"contract address",contractAddr)
			}
		case 2:
			// in block 3, call function for the contract
			parsed, err := abi.JSON(strings.NewReader(CoinABI))
			if err != nil {
				panic(fmt.Sprintf("Failed to parse abi %v", err))
			}
			for i:=0;i<sendNumber;i++ {
				addr1,addr2 := makeAddress(),makeAddress()
				input, _ := parsed.Pack("rechargeToAccount", addr1, addr2)
				value := abeyToWei(2)
				nonce := gen.TxNonce(delegateAddr[i])
				tx := makeCallTransaction(delegateKey[i], contracts[delegateAddr[i]], nonce, input, value,gspec.Config.ChainID)
				gen.AddTx(tx)
			}
		}
	})

	repeat := int64(2)
	for i := 0; i < int(repeat); i++ {
		db1 := abeydb.NewMemDatabase()
		gspec.MustFastCommit(db1)

		blockchain, err := core.NewBlockChain(db1, nil, gspec.Config, engine, vm.Config{})
		if err != nil {
			fmt.Println("NewBlockChain ", err)
		}
		blockchain.SetParallel(true)
		start := time.Now()
		if _, err := blockchain.InsertChain(chain); err != nil {
			panic(err)
		}
		fmt.Println("parallel execute ",i," cost time",time.Now().Sub(start))
	}
	fmt.Println("insert block for the in direct")
	for i := 0; i < int(repeat); i++ {
		db1 := abeydb.NewMemDatabase()
		gspec.MustFastCommit(db1)

		blockchain, err := core.NewBlockChain(db1, nil, gspec.Config, engine, vm.Config{})
		if err != nil {
			fmt.Println("NewBlockChain ", err)
		}
		blockchain.SetParallel(false)
		start := time.Now()
		if _, err := blockchain.InsertChain(chain); err != nil {
			panic(err)
		}
		fmt.Println("serial execute ",i," cost time",time.Now().Sub(start))
	}
}
func TestCmpCommonTransaction(t *testing.T) {
	setHighLevelForLog()
	params.MinTimeGap = big.NewInt(0)
	params.SnailRewardInterval = big.NewInt(3)

	sendNumber := 1000
	delegateKey := make([]*ecdsa.PrivateKey, sendNumber)
	delegateAddr := make([]common.Address, sendNumber)

	for i := 0; i < sendNumber; i++ {
		delegateKey[i], _ = crypto.GenerateKey()
		delegateAddr[i] = crypto.PubkeyToAddress(delegateKey[i].PublicKey)
	}

	gspec2 := DefaulGenesisBlock2(delegateAddr)
	genesis := gspec2.MustFastCommit(db)

	chain, _ := core.GenerateChain(gspec2.Config, genesis, engine, db, 10, func(i int, gen *core.BlockGen) {
		switch i {
		case 1:
			//In block 1, all transaction to the one address.
			total := makeAddress()
			for i := 0; i < sendNumber; i++ {
				nonce := gen.TxNonce(delegateAddr[i])
				value := abeyToWei(1)
				tx := makeTransaction(delegateKey[i], total, nonce, value,gspec2.Config.ChainID)
				gen.AddTx(tx)
			}
		case 5:
			// in block 3, one transaction to a new address
			for i:=0;i<sendNumber;i++ {
				addr := makeAddress()
				value := abeyToWei(1)
				nonce := gen.TxNonce(delegateAddr[i])
				tx := makeTransaction(delegateKey[i], addr, nonce, value,gspec2.Config.ChainID)
				gen.AddTx(tx)
			}
		}
	})

	repeat := int64(2)
	for i := 0; i < int(repeat); i++ {
		db1 := abeydb.NewMemDatabase()
		gspec2.MustFastCommit(db1)

		blockchain, err := core.NewBlockChain(db1, nil, gspec2.Config, engine, vm.Config{})
		if err != nil {
			fmt.Println("NewBlockChain ", err)
		}
		blockchain.SetParallel(true)
		start := time.Now()
		if _, err := blockchain.InsertChain(chain); err != nil {
			panic(err)
		}
		fmt.Println("parallel execute ",i," cost time",time.Now().Sub(start))
	}
	fmt.Println("insert block for the in direct")
	for i := 0; i < int(repeat); i++ {
		db1 := abeydb.NewMemDatabase()
		gspec2.MustFastCommit(db1)

		blockchain, err := core.NewBlockChain(db1, nil, gspec2.Config, engine, vm.Config{})
		if err != nil {
			fmt.Println("NewBlockChain ", err)
		}
		blockchain.SetParallel(false)
		start := time.Now()
		if _, err := blockchain.InsertChain(chain); err != nil {
			panic(err)
		}
		fmt.Println("serial execute ",i," cost time",time.Now().Sub(start))
	}
}
func TestBatchTxs(t *testing.T) {
	fmt.Println("test batch","sendcount",1000,"toBatch",10)
	batchTxs0(1000,10)
	fmt.Println("test batch","sendcount",1000,"toBatch",50)
	batchTxs0(1000,50)
	fmt.Println("test batch","sendcount",1000,"toBatch",100)
	batchTxs0(1000,100)

	fmt.Println("test batch","sendcount",5000,"toBatch",10)
	batchTxs0(5000,10)
	fmt.Println("test batch","sendcount",5000,"toBatch",50)
	batchTxs0(5000,50)
	fmt.Println("test batch","sendcount",5000,"toBatch",100)
	batchTxs0(5000,100)
	fmt.Println("test batch","sendcount",5000,"toBatch",200)
	batchTxs0(5000,200)
	fmt.Println("test batch","sendcount",5000,"toBatch",300)
	batchTxs0(5000,300)
	fmt.Println("test batch","sendcount",5000,"toBatch",500)
	batchTxs0(5000,500)
}

func batchTxs0(sendCount,toBatch int) {
	setHighLevelForLog()

	coinPriv,_ := crypto.GenerateKey()
	coinAddress := crypto.PubkeyToAddress(coinPriv.PublicKey)
	addrs := make(map[common.Address]*ecdsa.PrivateKey)

	gspec2 := DefaulGenesisBlock2([]common.Address{coinAddress})
	genesis := gspec2.MustFastCommit(db)

	chain, _ := core.GenerateChain(gspec2.Config, genesis, engine, db, 10, func(i int, gen *core.BlockGen) {
		switch i {
		case 1:
			//In block 1, one address transaction to a new address.
			for i := 0; i < sendCount; i++ {
				priv,newAddress := makeAddress2()
				nonce := gen.TxNonce(coinAddress)
				value := abeyToWei(100)
				tx := makeTransaction(coinPriv, newAddress, nonce, value,gspec2.Config.ChainID)
				gen.AddTx(tx)
				addrs[newAddress] = priv
			}
		case 3:
			// in block 3, batch addresses transaction to a new address
			count := 0
			oneAddress := makeAddress()
			for addr,priv := range addrs {
				if count % toBatch == 0 {
					oneAddress = makeAddress()
				}
				count++
				value := abeyToWei(1)
				nonce := gen.TxNonce(addr)
				tx := makeTransaction(priv, oneAddress, nonce, value,gspec2.Config.ChainID)
				gen.AddTx(tx)
			}
		}
	})

	repeat := int64(2)
	for i := 0; i < int(repeat); i++ {
		db1 := abeydb.NewMemDatabase()
		gspec2.MustFastCommit(db1)

		blockchain, err := core.NewBlockChain(db1, nil, gspec2.Config, engine, vm.Config{})
		if err != nil {
			fmt.Println("NewBlockChain ", err)
		}
		blockchain.SetParallel(true)
		start := time.Now()
		if _, err := blockchain.InsertChain(chain); err != nil {
			panic(err)
		}
		fmt.Println("parallel execute ",i," cost time",time.Now().Sub(start))
	}
	fmt.Println("insert block for the in direct")
	for i := 0; i < int(repeat); i++ {
		db1 := abeydb.NewMemDatabase()
		gspec2.MustFastCommit(db1)

		blockchain, err := core.NewBlockChain(db1, nil, gspec2.Config, engine, vm.Config{})
		if err != nil {
			fmt.Println("NewBlockChain ", err)
		}
		blockchain.SetParallel(false)
		start := time.Now()
		if _, err := blockchain.InsertChain(chain); err != nil {
			panic(err)
		}
		fmt.Println("serial execute ",i," cost time",time.Now().Sub(start))
	}
}
///////////////////////////////////////////////////////////////////////
func makeContractTransaction(key *ecdsa.PrivateKey, nonce uint64, data []byte,chainID *big.Int) (*types.Transaction, common.Address) {
	opts := bind.NewKeyedTransactor(key)
	value := new(big.Int)
	gasLimit := uint64(500000)
	gasPrice := big.NewInt(10000)

	rawTx := types.NewContractCreation(nonce, value, gasLimit, gasPrice, data)
	signer := types.NewTIP1Signer(chainID)
	signedTx, _ := opts.Signer(signer, opts.From, rawTx)
	address := crypto.CreateAddress(opts.From, signedTx.Nonce())

	return signedTx, address
}

func makeCallTransaction(key *ecdsa.PrivateKey, to common.Address, nonce uint64, input []byte, value *big.Int,chainID *big.Int) *types.Transaction {
	opts := bind.NewKeyedTransactor(key)
	gasLimit := uint64(500000)
	gasPrice := big.NewInt(10000)

	rawTx := types.NewTransaction(nonce, to, value, gasLimit, gasPrice, input)
	signer := types.NewTIP1Signer(chainID)
	signedTx, _ := opts.Signer(signer, opts.From, rawTx)

	return signedTx
}
func makeTransaction(key *ecdsa.PrivateKey, to common.Address, nonce uint64,value *big.Int,chainID *big.Int) *types.Transaction {
	opts := bind.NewKeyedTransactor(key)
	gasLimit := uint64(500000)
	gasPrice := big.NewInt(10000)

	rawTx := types.NewTransaction(nonce, to, value, gasLimit, gasPrice, nil)
	signer := types.NewTIP1Signer(chainID)
	signedTx, _ := opts.Signer(signer, opts.From, rawTx)

	return signedTx
}
///////////////////////////////////////////////////////////////////////
func abeyToWei(vaule uint64) *big.Int {
	baseUnit := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	value := new(big.Int).Mul(big.NewInt(int64(vaule)), baseUnit)
	return value
}
func weiToAbey(value *big.Int) uint64 {
	baseUnit := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	valueT := new(big.Int).Div(value, baseUnit).Uint64()
	return valueT
}
func makeAddress() common.Address {
	key, _ := crypto.GenerateKey()
	return  crypto.PubkeyToAddress(key.PublicKey)
}
func makeAddress2() (*ecdsa.PrivateKey,common.Address) {
	key, _ := crypto.GenerateKey()
	return  key,crypto.PubkeyToAddress(key.PublicKey)
}
///////////////////////////////////////////////////////////////////////