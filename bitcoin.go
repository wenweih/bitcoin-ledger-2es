package main

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	log "github.com/sirupsen/logrus"
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
		log.Fatal(err.Error())
	}
	return client
}

func (btcClient *bitcoinClientAlias) BTCReSetSync(hightest int32, elasticClient *elasticClientAlias) {
	names, err := elasticClient.IndexNames()
	ctx := context.Background()
	if err != nil {
		log.Fatalln(err.Error())
	}

	for _, name := range names {
		elasticClient.DeleteIndex(name).Do(ctx)
	}

	elasticClient.createIndices()
	// btcClient.BTCSync(ctx, int32(1), hightest, elasticClient)
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

// BTCBalance type struct
type BTCBalance struct {
	Address string  `json:"address"`
	Amount  float64 `json:"amount"`
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

// TxStream type struct
type TxStream struct {
	Txid      string                  `json:"txid"`
	Fee       string                  `json:"fee"`
	BlockHash string                  `json:"blockhash"`
	Time      int64                   `json:"time"`
	Vins      []*AddressWithValueInTx `json:"vins"`
	Vouts     []*AddressWithValueInTx `json:"vouts"`
}

type voutUsed struct {
	Txid     string `json:"txid"`
	VinIndex uint32 `json:"vinindex"`
}

// BTCBlockWithTxDetail elasticsearch 中 block Type 数据
func BTCBlockWithTxDetail(block *btcjson.GetBlockVerboseResult) interface{} {
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
		"tx":           block.Tx,
	}
	return blockWithTx
}

// BTCVoutAddress found address in bitcoin vout
func BTCVoutAddress(vout btcjson.Vout) (*[]string, error) {
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

// BTCVoutStream elasticsearch 中 voutstream Type 数据
func BTCVoutStream(vout btcjson.Vout, vins []btcjson.Vin, TxID string) *VoutStream {
	coinbase := false
	if len(vins[0].Coinbase) != 0 && len(vins[0].Txid) == 0 {
		coinbase = true
	}

	addresses, _ := BTCVoutAddress(vout)

	v := &VoutStream{
		TxIDBelongTo: TxID,
		Value:        vout.Value,
		Voutindex:    vout.N,
		Coinbase:     coinbase,
		Addresses:    *addresses,
		Used:         nil,
	}
	return v
}

// BTCTxStream elasticsearch 中 txstream Type 数据
func BTCTxStream(txid, blockHash, fee string, time int64, simpleVins, simpleVouts []*AddressWithValueInTx) *TxStream {
	result := &TxStream{
		Txid:      txid,
		Fee:       fee,
		BlockHash: blockHash,
		Time:      time, // TODO: time field is nil, need to fix
		Vins:      simpleVins,
		Vouts:     simpleVouts,
	}
	return result
}
