package main

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/shopspring/decimal"
)

type bitcoinClientAlias struct {
	*rpcclient.Client
}

func (conf *configure) bitcoinClient() *rpcclient.Client {
	connCfg := &rpcclient.ConnConfig{
		Host:         strings.Join([]string{conf.BitcoinHost, conf.BitcoinPort}, ":"),
		User:         conf.BitcoinUser,
		Pass:         conf.BitcoinPass,
		HTTPPostMode: conf.BitcoinhttpMode,
		DisableTLS:   conf.BitcoinDisableTLS,
	}

	client, err := rpcclient.New(connCfg, nil)
	if err != nil {
		sugar.Fatal("bitcoind client err: ", err.Error())
	}
	return client
}

func (btcClient *bitcoinClientAlias) ReSetSync(hightest int32, elasticClient *elasticClientAlias) {
	names, err := elasticClient.IndexNames()
	ctx := context.Background()
	if err != nil {
		sugar.Fatal("ctx error: ", err.Error())
	}

	for _, name := range names {
		elasticClient.DeleteIndex(name).Do(ctx)
	}

	elasticClient.createIndices()
	btcClient.SyncConcurrency(int32(1), hightest, elasticClient)
}

func (btcClient *bitcoinClientAlias) getBlock(height int32) (*btcjson.GetBlockVerboseResult, error) {
	complete := make(chan bool)
	blockHashCh := make(chan *chainhash.Hash)
	blockCh := make(chan *btcjson.GetBlockVerboseResult)
	totalTask := 2
	go func() {
		blockHash, err := btcClient.GetBlockHash(int64(height))
		if err != nil {
			close(blockHashCh)

			complete <- false
			return
		}
		blockHashCh <- blockHash
		complete <- true
	}()

	go func() {
		hash, ok := <-blockHashCh
		if ok {
			block, err := btcClient.GetBlockVerboseTxM(hash)
			if err != nil {
				complete <- false
				return
			}
			complete <- true
			blockCh <- block
		}
	}()

	for i := 0; i < totalTask; i++ {
		result := <-complete
		if !result {
			return nil, errors.New(strings.Join([]string{"Get block error, number:", strconv.Itoa(int(height))}, ""))
		}
	}

	block := <-blockCh
	return block, nil
}

// Balance type struct
type Balance struct {
	Address string  `json:"address"`
	Amount  float64 `json:"amount"`
}

// BalanceJournal 余额变更流水
type BalanceJournal struct {
	Address string  `json:"address"`
	Amount  float64 `json:"amount"`
	Operate string  `json:"operate"`
	Txid    string  `json:"txid"`
}

// AddressWithAmount 地址-余额类型
type AddressWithAmount struct {
	Address string          `json:"address"`
	Amount  decimal.Decimal `json:"amount"`
}

// BalanceWithID 类型
type BalanceWithID struct {
	ID      string  `json:"id"`
	Balance Balance `json:"balance"`
}

// VoutWithID type struct
type VoutWithID struct {
	ID   string
	Vout *VoutStream
}

// VoutStream type struct
type VoutStream struct {
	TxIDBelongTo string      `json:"txidbelongto"`
	Value        float64     `json:"value"`
	Voutindex    uint32      `json:"voutindex"`
	Coinbase     bool        `json:"coinbase"`
	Addresses    []string    `json:"addresses"`
	Used         interface{} `json:"used"`
}

// AddressWithValueInTx 交易中地输入输出的地址和余额
type AddressWithValueInTx struct {
	Address string  `json:"address"`
	Value   float64 `json:"value"`
}

// IndexUTXO vout 索引
type IndexUTXO struct {
	Txid  string
	Index uint32
}

// TxStream type struct
type esTx struct {
	Txid      string                 `json:"txid"`
	Fee       float64                `json:"fee"`
	BlockHash string                 `json:"blockhash"`
	Time      int64                  `json:"time"`
	Vins      []AddressWithValueInTx `json:"vins"`
	Vouts     []AddressWithValueInTx `json:"vouts"`
}

type voutUsed struct {
	Txid     string `json:"txid"`     // 所在交易的 id
	VinIndex uint32 `json:"vinindex"` // 作为 vin 被使用时，vin 的 vout 字段
}

// BTCBlockWithTxDetail elasticsearch 中 block Type 数据
func blockWithTxDetail(block *btcjson.GetBlockVerboseResult) interface{} {
	txs := blockTx(block.Tx)
	blockWithTx := map[string]interface{}{
		"hash":         block.Hash,
		"strippedsize": block.StrippedSize,
		"size":         block.Size,
		"weight":       block.Weight,
		"height":       block.Height,
		"versionHex":   block.VersionHex,
		"merkleroot":   block.MerkleRoot,
		"time":         block.Time,
		"nonce":        block.Nonce,
		"bits":         block.Bits,
		"difficulty":   block.Difficulty,
		"previoushash": block.PreviousHash,
		"nexthash":     block.NextHash,
		"tx":           txs,
	}
	return blockWithTx
}

func blockTx(txs []btcjson.TxRawResult) []map[string]interface{} {
	var rawTxs []map[string]interface{}
	for _, tx := range txs {
		// https://tradeblock.com/blog/bitcoin-0-8-5-released-provides-critical-bug-fixes/
		txVersion := tx.Version
		if tx.Version < 0 {
			txVersion = 1
		}
		vouts := txVouts(tx)
		vins := txVins(tx)
		rawTxs = append(rawTxs, map[string]interface{}{
			"txid":     tx.Txid,
			"hash":     tx.Hash,
			"version":  txVersion,
			"size":     tx.Size,
			"vsize":    tx.Vsize,
			"locktime": tx.LockTime,
			"vout":     vouts,
			"vin":      vins,
		})
	}
	return rawTxs
}

func txVouts(tx btcjson.TxRawResult) []map[string]interface{} {
	var vouts []map[string]interface{}
	for _, vout := range tx.Vout {
		vouts = append(vouts, map[string]interface{}{
			"value": vout.Value,
			"n":     vout.N,
			"scriptPubKey": map[string]interface{}{
				"asm":       vout.ScriptPubKey.Asm,
				"reqSigs":   vout.ScriptPubKey.ReqSigs,
				"type":      vout.ScriptPubKey.Type,
				"addresses": vout.ScriptPubKey.Addresses,
			},
		})
	}
	return vouts
}

func txVins(tx btcjson.TxRawResult) []map[string]interface{} {
	var vins []map[string]interface{}
	for _, vin := range tx.Vin {
		if len(tx.Vin) == 1 && len(vin.Coinbase) != 0 && len(vin.Txid) == 0 {
			vins = append(vins, map[string]interface{}{
				"coinbase": vin.Coinbase,
				"sequence": vin.Sequence,
			})
			break
		}
		vins = append(vins, map[string]interface{}{
			"txid": vin.Txid,
			"vout": vin.Vout,
			"scriptSig": map[string]interface{}{
				"asm": vin.ScriptSig.Asm,
			},
			"sequence": vin.Sequence,
		})
	}
	return vins
}

// get addresses in bitcoin vout
func voutAddressFun(vout btcjson.Vout) (*[]string, error) {
	var addresses []string
	if len(vout.ScriptPubKey.Addresses) > 0 {
		addresses = vout.ScriptPubKey.Addresses
		return &addresses, nil
	}
	if len(addresses) == 0 {
		return nil, errors.New("Unable to decode output address")
	}
	return nil, errors.New("address not fount in vout")
}

// VoutStream elasticsearch 中 voutstream Type 数据
func newVoutFun(vout btcjson.Vout, vins []btcjson.Vin, TxID string) (*VoutStream, error) {
	coinbase := false
	if len(vins[0].Coinbase) != 0 && len(vins[0].Txid) == 0 {
		coinbase = true
	}

	addresses, err := voutAddressFun(vout)
	if err != nil {
		return nil, err
	}

	v := &VoutStream{
		TxIDBelongTo: TxID,
		Value:        vout.Value,
		Voutindex:    vout.N,
		Coinbase:     coinbase,
		Addresses:    *addresses,
		Used:         nil,
	}
	return v, nil
}

func newBalanceJournalFun(address, ope, txid string, amount float64) BalanceJournal {
	balancejournal := BalanceJournal{
		Address: address,
		Operate: ope,
		Amount:  amount,
		Txid:    txid,
	}
	return balancejournal
}

//  elasticsearch 中 txstream Type 数据
func esTxFun(txid, blockHash string, fee float64, time int64, simpleVins, simpleVouts []AddressWithValueInTx) *esTx {
	result := &esTx{
		Txid:      txid,
		Fee:       fee,
		BlockHash: blockHash,
		Time:      time, // TODO: time field is nil, need to fix
		Vins:      simpleVins,
		Vouts:     simpleVouts,
	}
	return result
}

// return value:
// *[]*AddressWithValueInTx for elasticsearch tx Type vouts field
// *[]interface{} all addresses related to the vout
// *[]*Balance all addresses related to the vout with value amount
func parseTxVout(vout btcjson.Vout) ([]AddressWithValueInTx, []interface{}, []Balance) {
	var (
		txVoutsField           []AddressWithValueInTx
		voutAddresses          []interface{} // All addresses related with vout in a block
		voutAddressWithAmounts []Balance
	)
	// vouts field in tx type
	for _, address := range vout.ScriptPubKey.Addresses {
		txVoutsField = append(txVoutsField, AddressWithValueInTx{
			Address: address,
			Value:   vout.Value,
		})

		// vout addresses slice
		voutAddresses = append(voutAddresses, address)

		// vout addresses with amount
		voutAddressWithAmounts = append(voutAddressWithAmounts, Balance{address, vout.Value})
	}
	return txVoutsField, voutAddresses, voutAddressWithAmounts
}

// return value
// []*AddressWithValueInTx for elasticsearch tx Type vins field
// []interface{} all addresses related to the vin
// []*Balance all addresses related to the vout with value amount
func parseESVout(voutWithID VoutWithID) ([]AddressWithValueInTx, []interface{}, []Balance) {
	var (
		txTypeVinsField           []AddressWithValueInTx
		vinAddresses              []interface{}
		vinAddressWithAmountSlice []Balance
	)

	for _, address := range voutWithID.Vout.Addresses {
		vinAddresses = append(vinAddresses, address)
		vinAddressWithAmountSlice = append(vinAddressWithAmountSlice, Balance{address, voutWithID.Vout.Value})
		txTypeVinsField = append(txTypeVinsField, AddressWithValueInTx{address, voutWithID.Vout.Value})
	}
	return txTypeVinsField, vinAddresses, vinAddressWithAmountSlice
}

func indexedVinsFun(vins []btcjson.Vin) []IndexUTXO {
	var IndexUTXOs []IndexUTXO
	for _, vin := range vins {
		item := IndexUTXO{vin.Txid, vin.Vout}
		IndexUTXOs = append(IndexUTXOs, item)
	}
	return IndexUTXOs
}

func indexedVoutsFun(vouts []btcjson.Vout, txid string) []IndexUTXO {
	var IndexUTXOs []IndexUTXO
	for _, vout := range vouts {
		IndexUTXOs = append(IndexUTXOs, IndexUTXO{txid, vout.N})
	}
	return IndexUTXOs
}
