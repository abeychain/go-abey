package abeyclient

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	abeychain "github.com/abeychain/go-abey"
	"github.com/abeychain/go-abey/common"
	"github.com/abeychain/go-abey/core/types"
	"github.com/abeychain/go-abey/crypto"
	"github.com/abeychain/go-abey/params"
	"log"
	"math/big"
	"reflect"
	"testing"
	"time"
)

// Verify that Client implements the abeychain interfaces.
var (
	_ = abeychain.ChainReader(&Client{})
	_ = abeychain.TransactionReader(&Client{})
	_ = abeychain.ChainStateReader(&Client{})
	_ = abeychain.ChainSyncReader(&Client{})
	_ = abeychain.ContractCaller(&Client{})
	_ = abeychain.GasEstimator(&Client{})
	_ = abeychain.GasPricer(&Client{})
	_ = abeychain.LogFilterer(&Client{})
	_ = abeychain.PendingStateReader(&Client{})
	// _ = abeychain.PendingStateEventer(&Client{})
	_ = abeychain.PendingContractCaller(&Client{})
)

func TestToFilterArg(t *testing.T) {
	blockHashErr := fmt.Errorf("cannot specify both BlockHash and FromBlock/ToBlock")
	addresses := []common.Address{
		common.HexToAddress("0xD36722ADeC3EdCB29c8e7b5a47f352D701393462"),
	}
	blockHash := common.HexToHash(
		"0xeb94bb7d78b73657a9d7a99792413f50c0a45c51fc62bdcb08a53f18e9a2b4eb",
	)

	for _, testCase := range []struct {
		name   string
		input  abeychain.FilterQuery
		output interface{}
		err    error
	}{
		{
			"without BlockHash",
			abeychain.FilterQuery{
				Addresses: addresses,
				FromBlock: big.NewInt(1),
				ToBlock:   big.NewInt(2),
				Topics:    [][]common.Hash{},
			},
			map[string]interface{}{
				"address":   addresses,
				"fromBlock": "0x1",
				"toBlock":   "0x2",
				"topics":    [][]common.Hash{},
			},
			nil,
		},
		{
			"with nil fromBlock and nil toBlock",
			abeychain.FilterQuery{
				Addresses: addresses,
				Topics:    [][]common.Hash{},
			},
			map[string]interface{}{
				"address":   addresses,
				"fromBlock": "0x0",
				"toBlock":   "latest",
				"topics":    [][]common.Hash{},
			},
			nil,
		},
		{
			"with negative fromBlock and negative toBlock",
			abeychain.FilterQuery{
				Addresses: addresses,
				FromBlock: big.NewInt(-1),
				ToBlock:   big.NewInt(-1),
				Topics:    [][]common.Hash{},
			},
			map[string]interface{}{
				"address":   addresses,
				"fromBlock": "pending",
				"toBlock":   "pending",
				"topics":    [][]common.Hash{},
			},
			nil,
		},
		{
			"with blockhash",
			abeychain.FilterQuery{
				Addresses: addresses,
				BlockHash: &blockHash,
				Topics:    [][]common.Hash{},
			},
			map[string]interface{}{
				"address":   addresses,
				"blockHash": blockHash,
				"topics":    [][]common.Hash{},
			},
			nil,
		},
		{
			"with blockhash and from block",
			abeychain.FilterQuery{
				Addresses: addresses,
				BlockHash: &blockHash,
				FromBlock: big.NewInt(1),
				Topics:    [][]common.Hash{},
			},
			nil,
			blockHashErr,
		},
		{
			"with blockhash and to block",
			abeychain.FilterQuery{
				Addresses: addresses,
				BlockHash: &blockHash,
				ToBlock:   big.NewInt(1),
				Topics:    [][]common.Hash{},
			},
			nil,
			blockHashErr,
		},
		{
			"with blockhash and both from / to block",
			abeychain.FilterQuery{
				Addresses: addresses,
				BlockHash: &blockHash,
				FromBlock: big.NewInt(1),
				ToBlock:   big.NewInt(2),
				Topics:    [][]common.Hash{},
			},
			nil,
			blockHashErr,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			output, err := toFilterArg(testCase.input)
			if (testCase.err == nil) != (err == nil) {
				t.Fatalf("expected error %v but got %v", testCase.err, err)
			}
			if testCase.err != nil {
				if testCase.err.Error() != err.Error() {
					t.Fatalf("expected error %v but got %v", testCase.err, err)
				}
			} else if !reflect.DeepEqual(testCase.output, output) {
				t.Fatalf("expected filter arg %v but got %v", testCase.output, output)
			}
		})
	}
}

var (
	testKey, _  = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	testAddr    = crypto.PubkeyToAddress(testKey.PublicKey)
	testBalance = big.NewInt(2e15)
	payerKey, _ = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
	payerAddr   = crypto.PubkeyToAddress(payerKey.PublicKey)
	txFee       = big.NewInt(1e17)
	// "http://18.138.171.105:7545"  "https://rpc.abeychain.com"
	devUrl = "http://18.138.171.105:7545"
)

func makeTransaction(nonce uint64) *types.Transaction {
	return types.NewTransaction(
		nonce, common.Address{80}, big.NewInt(4000000), 50000, big.NewInt(int64(50*params.GWei)), nil)
}
func makePayerTransaction(nonce uint64) *types.Transaction {
	return types.NewTransaction_Payment(nonce, common.Address{81}, big.NewInt(5000000), txFee, 50000,
		big.NewInt(int64(50*params.GWei)), nil, payerAddr)
}

func firstSetup(ec *Client) error {
	devGenesisKey, err := crypto.HexToECDSA("55dcdfd62f565a66e1886959e82a365e4987ed0b405adc43614a42c3481edd1a")
	if err != nil {
		return err
	}
	addr0 := crypto.PubkeyToAddress(devGenesisKey.PublicKey)

	num, e := ec.BlockNumber(context.Background())
	if e != nil {
		panic(e)
	}
	fmt.Println("current block number is", num, "is tip10", params.DevnetChainConfig.IsTIP10(big.NewInt(int64(num))))

	b, e := ec.BalanceAt(context.Background(), addr0, nil)
	if e != nil {
		fmt.Println(e)
		return e
	}
	fmt.Println("genesis address balance", types.ToAbey(b).String())
	b, e = ec.BalanceAt(context.Background(), testAddr, nil)
	if e != nil {
		fmt.Println(e)
		return e
	}
	fmt.Println("testkey balance", types.ToAbey(b).String())

	b, e = ec.BalanceAt(context.Background(), payerAddr, nil)
	if e != nil {
		fmt.Println(e)
		return e
	}
	fmt.Println("payerAddr", types.ToAbey(b).String())

	amount := new(big.Int).Mul(big.NewInt(5000), big.NewInt(1e18))
	nonce, e := ec.PendingNonceAt(context.Background(), addr0)
	if e != nil {
		return e
	}
	tx0 := types.NewTransaction(nonce, testAddr, amount, 30000, big.NewInt(int64(50*params.GWei)), nil)
	tx1 := types.NewTransaction(nonce+1, payerAddr, amount, 30000, big.NewInt(int64(50*params.GWei)), nil)

	e = sendTransaction(ec, tx0, devGenesisKey)
	if e != nil {
		return e
	}
	e = sendTransaction(ec, tx1, devGenesisKey)
	if e != nil {
		return e
	}

	time.Sleep(time.Second * 3)
	b, e = ec.BalanceAt(context.Background(), testAddr, nil)
	if e != nil {
		fmt.Println(e)
		return e
	}
	fmt.Println("testkey balance", types.ToAbey(b).String())

	b, e = ec.BalanceAt(context.Background(), payerAddr, nil)
	if e != nil {
		fmt.Println(e)
		return e
	}
	fmt.Println("payerAddr balance", types.ToAbey(b).String())
	return nil
}

func sendTransaction(ec *Client, tx *types.Transaction, prv *ecdsa.PrivateKey) error {
	chainID, err := ec.ChainID(context.Background())
	if err != nil {
		return err
	}
	signer := types.NewTIP1Signer(chainID)
	tx, err = types.SignTx(tx, signer, prv)
	if err != nil {
		return err
	}
	err = ec.SendTransaction(context.Background(), tx)
	if err != nil {
		return err
	}
	time.Sleep(time.Second * 7)
	fmt.Println("send transaction end,the txhash", tx.Hash().Hex())

	receipt, err := ec.TransactionReceipt(context.Background(), tx.Hash())
	if err != nil {
		fmt.Println("send transaction end,use the old txhash", tx.HashOld().Hex())
		receipt, err = ec.TransactionReceipt(context.Background(), tx.HashOld())
		if err != nil {
			return err
		}
	}

	if receipt.Status == types.ReceiptStatusSuccessful {
		block, err := ec.BlockByHash(context.Background(), receipt.BlockHash)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Transaction Success", " block Number", receipt.BlockNumber.Uint64(),
			" block txs", len(block.Transactions()), "blockhash", block.Hash().Hex())
	} else if receipt.Status == types.ReceiptStatusFailed {
		fmt.Println("Transaction Failed ", " Block Number", receipt.BlockNumber.Uint64())
	}
	return nil
}
func sendPayerTransaction(ec *Client, tx *types.Transaction, prv, prvPayer *ecdsa.PrivateKey) error {
	chainID, err := ec.ChainID(context.Background())
	if err != nil {
		return err
	}
	signer := types.NewTIP1Signer(chainID)
	tx, err = types.SignTx(tx, signer, prv)
	if err != nil {
		return err
	}
	tx, err = types.SignTx_Payment(tx, signer, payerKey)
	if err != nil {
		return err
	}

	err = ec.SendTransaction(context.Background(), tx)
	if err != nil {
		return err
	}

	time.Sleep(time.Second * 7)
	fmt.Println("send payer transaction end,the txhash", tx.Hash().Hex())

	receipt, err := ec.TransactionReceipt(context.Background(), tx.Hash())
	if err != nil {
		return err
	}

	if receipt.Status == types.ReceiptStatusSuccessful {
		block, err := ec.BlockByHash(context.Background(), receipt.BlockHash)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Transaction Success", " block Number", receipt.BlockNumber.Uint64(),
			" block txs", len(block.Transactions()), "blockhash", block.Hash().Hex())
	} else if receipt.Status == types.ReceiptStatusFailed {
		fmt.Println("Transaction Failed ", " Block Number", receipt.BlockNumber.Uint64())
	}
	return nil
}

func TestSetup(t *testing.T) {
	ec, err := Dial(devUrl)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = firstSetup(ec)
	if err != nil {
		fmt.Println(err)
		return
	}
}
func transTest(ec *Client) {

	num, e := ec.BlockNumber(context.Background())
	if e != nil {
		panic(e)
	}
	fmt.Println("current block number is", num, "is tip10", params.DevnetChainConfig.IsTIP10(big.NewInt(int64(num))))

	fmt.Println("send common tx......")
	nonce, e := ec.PendingNonceAt(context.Background(), testAddr)
	if e != nil {
		panic(e)
	}
	tx := makeTransaction(nonce)
	err := sendTransaction(ec, tx, testKey)
	if err != nil {
		panic(err)
	}

	fmt.Println("send payer tx.......")

	nonce, e = ec.PendingNonceAt(context.Background(), testAddr)
	if e != nil {
		panic(e)
	}
	tx = makePayerTransaction(nonce)
	err = sendPayerTransaction(ec, tx, testKey, payerKey)
	if err != nil {
		panic(err)
	}
}

func queryTest(ec *Client) {

	num, e := ec.BlockNumber(context.Background())
	if e != nil {
		panic(e)
	}
	fmt.Println("current block number is", num, "is tip10", params.DevnetChainConfig.IsTIP10(big.NewInt(int64(num))))
	// tx0 = "0xd902aac85aaa9522f29eadbe913b97d9d24edc2fdbf5ed45b96ef886959f1ddb"
	txstr0 := "0xa66d3fbbcccb438b4b6edc11984b80d2b8d34554d0a1244311494308d4c03e13"
	txhash0 := common.HexToHash(txstr0)

	tx0, pending, err := ec.TransactionByHash(context.Background(), txhash0)
	if err != nil {
		panic(err)
	}
	fmt.Println("pending", pending)
	fmt.Println("tx0", tx0.Info())
	receipt, err := ec.TransactionReceipt(context.Background(), txhash0)
	if err != nil {
		panic(err)
	}
	fmt.Println("tx in the block", receipt.BlockNumber)
}

func Test0(t *testing.T) {
	ec, _ := Dial(devUrl)

	transTest(ec)
}

func Test1(t *testing.T) {
	ec, _ := Dial(devUrl)

	queryTest(ec)
}
func Test2(t *testing.T) {
	allocAmount := new(big.Int).Mul(big.NewInt(990000000), big.NewInt(1e18))
	i, _ := new(big.Int).SetString("90000000000000000000000", 10)
	res := allocAmount.Cmp(i)
	fmt.Println(res)
}
func Test3(t *testing.T) {
	ec, _ := Dial(devUrl)
	num := params.DevnetChainConfig.TIP10.FastNumber
	err := traceBlocks(ec, num)
	if err != nil {
		fmt.Println(err)
	}
}
func traceBlocks(ec *Client, height *big.Int) error {
	//latest, e := ec.BlockNumber(context.Background())
	//if e != nil {
	//	return e
	//}
	for i := int64(1); i < height.Int64(); i++ {
		block, err := ec.BlockByNumber(context.Background(), big.NewInt(i))
		if err != nil {
			fmt.Println("get block err,height", i, "err", err)
		}
		txs := block.Transactions()
		if len(txs) > 0 {
			fmt.Println("========================================")
			fmt.Println("block height", block.Number().Uint64(), " has txs", len(txs))
			for ii, tx := range txs {
				fmt.Println("index", ii, "oldhash", tx.HashOld().Hex(), "newhash", tx.Hash().Hex(), "is payer", tx.Payer() != nil)
				fmt.Println("tx details", tx.Info())
			}
			fmt.Println("block header:", block.Header())
			fmt.Println("block body:", block.Body())
			fmt.Println("========================================")
		}
	}
	return nil
}
