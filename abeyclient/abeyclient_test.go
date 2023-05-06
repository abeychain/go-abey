package abeyclient

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	abeychain "github.com/abeychain/go-abey"
	"github.com/abeychain/go-abey/common"
	"github.com/abeychain/go-abey/core/types"
	"github.com/abeychain/go-abey/crypto"
	"github.com/abeychain/go-abey/node"
	"github.com/abeychain/go-abey/params"
	"github.com/abeychain/go-abey/rpc"
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
)

var testTx1 = MustSignNewTx(testKey, types.NewTIP1Signer(params.TestChainConfig.ChainID), types.NewTransaction(
	0, common.Address{2}, big.NewInt(100), 10000, big.NewInt(int64(params.TxGas)), nil))

var testTx2 = MustSignNewTx(testKey, types.NewTIP1Signer(params.TestChainConfig.ChainID), types.NewTransaction(
	1, common.Address{3}, big.NewInt(100), 10000, big.NewInt(int64(params.TxGas)), nil))

// MustSignNewTx creates a transaction and signs it.
// This panics if the transaction cannot be signed.
func MustSignNewTx(prv *ecdsa.PrivateKey, s types.Signer, tx *types.Transaction) *types.Transaction {
	tx, err := signNewTx(prv, s, tx)
	if err != nil {
		panic(err)
	}
	return tx
}
func signNewTx(prv *ecdsa.PrivateKey, s types.Signer, tx *types.Transaction) (*types.Transaction, error) {
	h := tx.Hash()
	sig, err := crypto.Sign(h[:], prv)
	if err != nil {
		return nil, err
	}
	return tx.WithSignature(s, sig)
}

func makeSignTransaction(chainid *big.Int, nonce uint64) (*types.Transaction, error) {
	return MustSignNewTx(testKey, types.NewTIP1Signer(chainid), types.NewTransaction(
		nonce, common.Address{80}, big.NewInt(4000000), 50000, big.NewInt(int64(params.TxGas)), nil)), nil
}
func makeSignPayerTransaction(chainid *big.Int, nonce uint64) (*types.Transaction, error) {
	tx := types.NewTransaction_Payment(nonce, common.Address{81}, big.NewInt(5000000), txFee, 50000,
		big.NewInt(int64(params.TxGas)), nil, payerAddr)
	signer := types.NewTIP1Signer(chainid)
	tx, err := types.SignTx(tx, signer, testKey)
	if err != nil {
		return nil, err
	}
	fmt.Println("txhash001", tx.Hash().Hex())
	tx, err = types.SignTx_Payment(tx, signer, payerKey)
	if err != nil {
		return nil, err
	}
	fmt.Println("txhash002", tx.Hash().Hex())
	return tx, nil
}

func firstSetup(ec *Client) error {
	devGenesisKey, _ := crypto.HexToECDSA("55dcdfd62f565a66e1886959e82a365e4987ed0b405adc43614a42c3481edd1a")
	addr0 := crypto.PubkeyToAddress(devGenesisKey.PublicKey)
	chainID, err := ec.ChainID(context.Background())
	if err != nil {
		return err
	}

	b, e := ec.BalanceAt(context.Background(), addr0, nil)
	if e != nil {
		return e
	}
	fmt.Println("genesis key balance", b.String())
	amount := new(big.Int).Mul(big.NewInt(5000), big.NewInt(1e18))
	nonce, e := ec.NonceAt(context.Background(), addr0, nil)
	if e != nil {
		return e
	}
	tx0 := types.NewTransaction(nonce, testAddr, amount, 30000, big.NewInt(int64(params.TxGas)), nil)
	tx1 := types.NewTransaction(nonce+1, payerAddr, amount, 30000, big.NewInt(int64(params.TxGas)), nil)
	tx0, e = types.SignTx(tx0, types.NewTIP1Signer(chainID), devGenesisKey)
	if e != nil {
		return err
	}
	tx1, e = types.SignTx(tx1, types.NewTIP1Signer(chainID), devGenesisKey)
	if e != nil {
		return err
	}

	err = ec.SendTransaction(context.Background(), tx0)
	if err != nil {
		return err
	}
	receipt0, err := ec.TransactionReceipt(context.Background(), tx0.Hash())
	if err != nil {
		return err
	}
	if receipt0.Status == types.ReceiptStatusSuccessful {
		block, err := ec.BlockByHash(context.Background(), receipt0.BlockHash)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Transaction Success", " block Number", receipt0.BlockNumber.Uint64(),
			" block txs", len(block.Transactions()), "blockhash", block.Hash().Hex())
	} else if receipt0.Status == types.ReceiptStatusFailed {
		fmt.Println("Transaction Failed ", " Block Number", receipt0.BlockNumber.Uint64())
	}

	err = ec.SendTransaction(context.Background(), tx1)
	if err != nil {
		return err
	}
	receipt1, err := ec.TransactionReceipt(context.Background(), tx0.Hash())
	if err != nil {
		return err
	}
	if receipt1.Status == types.ReceiptStatusSuccessful {
		block, err := ec.BlockByHash(context.Background(), receipt1.BlockHash)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Transaction Success", " block Number", receipt1.BlockNumber.Uint64(),
			" block txs", len(block.Transactions()), "blockhash", block.Hash().Hex())
	} else if receipt1.Status == types.ReceiptStatusFailed {
		fmt.Println("Transaction Failed ", " Block Number", receipt1.BlockNumber.Uint64())
	}
	return nil
}

func newTestBackend(t *testing.T) *node.Node {
	// Create node
	n, err := node.New(&node.Config{})
	if err != nil {
		t.Fatalf("can't create new node: %v", err)
	}
	// Create Ethereum Service

	// Import the test chain.
	if err := n.Start(); err != nil {
		t.Fatalf("can't start test node: %v", err)
	}

	return n
}

func TestEthClient(t *testing.T) {
	backend := newTestBackend(t)
	client, _ := backend.Attach()
	defer backend.Close()
	defer client.Close()

	tests := map[string]struct {
		test func(t *testing.T)
	}{
		"BalanceAt": {
			func(t *testing.T) { testBalanceAt(t, client) },
		},
		"TxInBlockInterrupted": {
			func(t *testing.T) { testTransactionInBlockInterrupted(t, client) },
		},
		"GetBlock": {
			func(t *testing.T) { testGetBlock(t, client) },
		},
		"CallContract": {
			func(t *testing.T) { testCallContract(t, client) },
		},
		"TransactionSender": {
			func(t *testing.T) { testTransactionSender(t, client) },
		},
	}

	t.Parallel()
	for name, tt := range tests {
		t.Run(name, tt.test)
	}
}

func testBalanceAt(t *testing.T, client *rpc.Client) {
	tests := map[string]struct {
		account common.Address
		block   *big.Int
		want    *big.Int
		wantErr error
	}{
		"valid_account_genesis": {
			account: testAddr,
			block:   big.NewInt(0),
			want:    testBalance,
		},
		"valid_account": {
			account: testAddr,
			block:   big.NewInt(1),
			want:    testBalance,
		},
		"non_existent_account": {
			account: common.Address{1},
			block:   big.NewInt(1),
			want:    big.NewInt(0),
		},
		"future_block": {
			account: testAddr,
			block:   big.NewInt(1000000000),
			want:    big.NewInt(0),
			wantErr: errors.New("header not found"),
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ec := NewClient(client)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			got, err := ec.BalanceAt(ctx, tt.account, tt.block)
			if tt.wantErr != nil && (err == nil || err.Error() != tt.wantErr.Error()) {
				t.Fatalf("BalanceAt(%x, %v) error = %q, want %q", tt.account, tt.block, err, tt.wantErr)
			}
			if got.Cmp(tt.want) != 0 {
				t.Fatalf("BalanceAt(%x, %v) = %v, want %v", tt.account, tt.block, got, tt.want)
			}
		})
	}
}

func testTransactionInBlockInterrupted(t *testing.T, client *rpc.Client) {
	ec := NewClient(client)

	// Get current block by number.
	block, err := ec.BlockByNumber(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Test tx in block interupted.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	tx, err := ec.TransactionInBlock(ctx, block.Hash(), 0)
	if tx != nil {
		t.Fatal("transaction should be nil")
	}
	if err == nil || err == abeychain.NotFound {
		t.Fatal("error should not be nil/notfound")
	}

	// Test tx in block not found.
	if _, err := ec.TransactionInBlock(context.Background(), block.Hash(), 20); err != abeychain.NotFound {
		t.Fatal("error should be abeychain.NotFound")
	}
}

func testGetBlock(t *testing.T, client *rpc.Client) {
	ec := NewClient(client)

	// Get current block number
	blockNumber, err := ec.BlockNumber(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if blockNumber != 2 {
		t.Fatalf("BlockNumber returned wrong number: %d", blockNumber)
	}
	// Get current block by number
	block, err := ec.BlockByNumber(context.Background(), new(big.Int).SetUint64(blockNumber))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if block.NumberU64() != blockNumber {
		t.Fatalf("BlockByNumber returned wrong block: want %d got %d", blockNumber, block.NumberU64())
	}
	// Get current block by hash
	blockH, err := ec.BlockByHash(context.Background(), block.Hash())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if block.Hash() != blockH.Hash() {
		t.Fatalf("BlockByHash returned wrong block: want %v got %v", block.Hash().Hex(), blockH.Hash().Hex())
	}
	// Get header by number
	header, err := ec.HeaderByNumber(context.Background(), new(big.Int).SetUint64(blockNumber))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if block.Header().Hash() != header.Hash() {
		t.Fatalf("HeaderByNumber returned wrong header: want %v got %v", block.Header().Hash().Hex(), header.Hash().Hex())
	}
	// Get header by hash
	headerH, err := ec.HeaderByHash(context.Background(), block.Hash())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if block.Header().Hash() != headerH.Hash() {
		t.Fatalf("HeaderByHash returned wrong header: want %v got %v", block.Header().Hash().Hex(), headerH.Hash().Hex())
	}
}

func testCallContract(t *testing.T, client *rpc.Client) {
	ec := NewClient(client)

	// EstimateGas
	msg := abeychain.CallMsg{
		From:  testAddr,
		To:    &common.Address{},
		Gas:   21000,
		Value: big.NewInt(1),
	}
	gas, err := ec.EstimateGas(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gas != 21000 {
		t.Fatalf("unexpected gas price: %v", gas)
	}
	// CallContract
	if _, err := ec.CallContract(context.Background(), msg, big.NewInt(1)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// PendingCallContract
	if _, err := ec.PendingCallContract(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func testTransactionSender(t *testing.T, client *rpc.Client) {
	ec := NewClient(client)
	ctx := context.Background()

	// Retrieve testTx1 via RPC.
	block2, err := ec.HeaderByNumber(ctx, big.NewInt(2))
	if err != nil {
		t.Fatal("can't get block 1:", err)
	}
	tx1, err := ec.TransactionInBlock(ctx, block2.Hash(), 0)
	if err != nil {
		t.Fatal("can't get tx:", err)
	}
	if tx1.Hash() != testTx1.Hash() {
		t.Fatalf("wrong tx hash %v, want %v", tx1.Hash(), testTx1.Hash())
	}

	// The sender address is cached in tx1, so no additional RPC should be required in
	// TransactionSender. Ensure the server is not asked by canceling the context here.
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	sender1, err := ec.TransactionSender(canceledCtx, tx1, block2.Hash(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if sender1 != testAddr {
		t.Fatal("wrong sender:", sender1)
	}

	// Now try to get the sender of testTx2, which was not fetched through RPC.
	// TransactionSender should query the server here.
	sender2, err := ec.TransactionSender(ctx, testTx2, block2.Hash(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if sender2 != testAddr {
		t.Fatal("wrong sender:", sender2)
	}
}

func sendTransaction(ec *Client) error {
	chainID, err := ec.ChainID(context.Background())
	if err != nil {
		return err
	}
	nonce, err := ec.PendingNonceAt(context.Background(), testAddr)
	if err != nil {
		return err
	}

	signer := types.NewTIP1Signer(chainID)
	tx, err := signNewTx(testKey, signer, types.NewTransaction(
		nonce, common.Address{3}, big.NewInt(100), 22000, big.NewInt(int64(params.TxGas)), nil))

	if err != nil {
		return err
	}
	err = ec.SendTransaction(context.Background(), tx)
	if err != nil {
		return err
	}
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
