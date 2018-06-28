package main

import (
	"context"
	"fmt"
	"strconv"

	log "github.com/sirupsen/logrus"

	"github.com/btcsuite/btcd/btcjson"
)

func sync() {
	eClient, err := config.elasticClient()
	if err != nil {
		log.Fatalf(err.Error())
	}

	eClient.createIndices()

	c := config.bitcoinClient()
	btcClient := bitcoinClientAlias{c}
	info, err := btcClient.GetBlockChainInfo()
	if err != nil {
		log.Fatalln(err.Error())
	}
	var DBCurrentHeight float64
	if agg, err := eClient.MaxAgg("height", "block", "block"); err != nil {
		btcClient.BTCReSetSync(info.Headers, eClient)
	} else {
		DBCurrentHeight = *agg
	}

	syncIndex := strconv.FormatFloat(DBCurrentHeight-5, 'f', -1, 64)
	if SyncBeginRecord, err := eClient.Get().Index("block").Type("block").Id(syncIndex).Do(context.Background()); err != nil || !SyncBeginRecord.Found {
		btcClient.BTCReSetSync(info.Headers, eClient)
	} else {
		// 数据库倒退 5 个块再同步
		ctx := context.Background()
		btcClient.BTCSync(ctx, int32(DBCurrentHeight-5), info.Headers, eClient)
	}
}

func (btcClient *bitcoinClientAlias) BTCSync(ctx context.Context, from, end int32, elasticClient *elasticClientAlias) {
	for height := from; height < end; height++ {
		blockHash, _ := btcClient.GetBlockHash(int64(height))
		if block, err := btcClient.GetBlockVerboseTxM(blockHash); err != nil {
			log.Fatalf(err.Error())
		} else {
			// 这个地址交易数据比较明显， 结合 https://blockchain.info/address/12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S 的交易数据测试验证同步逻辑 (该地址上 2009 年的交易数据)
			elasticClient.BTCRollBackAndSyncTx(ctx, from, height, block)
			elasticClient.BTCRollBackAndSyncBlock(ctx, from, height, block)
		}
	}
}

func (client *elasticClientAlias) BTCRollBackAndSyncBlock(ctx context.Context, from, height int32, block *btcjson.GetBlockVerboseResult) {
	bodyParams := BTCBlockWithTxDetail(block)
	if height < (from + 5) {
		// https://github.com/olivere/elastic/blob/release-branch.v6/update_test.go
		// https://www.elastic.co/guide/en/elasticsearch/reference/5.2/docs-update.html
		DocParams := BTCBlockWithTxDetail(block)
		client.Update().Index("block").Type("block").Id(strconv.FormatInt(int64(height), 10)).
			Doc(DocParams).DocAsUpsert(true).Upsert(DocParams).DetectNoop(true).Refresh("true").Do(ctx)
		fmt.Printf("sync update btc  %d %s\n", block.Height, block.Hash)
	} else {
		client.Index().Index("block").Type("block").Id(strconv.FormatInt(int64(height), 10)).BodyJson(bodyParams).Do(ctx)
		client.Flush()
		fmt.Printf("sync btc  %d %s\n", block.Height, block.Hash)
	}
}
