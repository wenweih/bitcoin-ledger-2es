package main

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/olivere/elastic"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"go.uber.org/zap"

	"github.com/btcsuite/btcd/btcjson"
)

// Sync dump bitcoin chaindata to es
func Sync() {
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

	// // 134680
	// fmt.Println(info)
	// block, _ := btcClient.getBlock(int32(143878))
	// if err := eClient.syncTx(context.Background(), int32(143878), block); err != nil {
	// 	return
	// }
	//
	// btcClient.SyncConcurrency(int32(134679), info.Headers, eClient)

	var DBCurrentHeight float64
	agg, err := eClient.MaxAgg("height", "block", "block")
	if err != nil {
		log.Warnln(err.Error(), ",resync from genesis block")
		btcClient.BTCReSetSync(info.Headers, eClient)
		return
	}
	DBCurrentHeight = *agg
	syncIndex := strconv.FormatFloat(DBCurrentHeight-5, 'f', -1, 64)
	if SyncBeginRecord, err := eClient.Get().Index("block").Type("block").Id(syncIndex).Do(context.Background()); err != nil || !SyncBeginRecord.Found {
		btcClient.BTCReSetSync(info.Headers, eClient)
	} else {
		// 数据库倒退 5 个块再同步
		btcClient.SyncConcurrency(int32(DBCurrentHeight-5), info.Headers, eClient)
	}
}

func (btcClient *bitcoinClientAlias) SyncConcurrency(from, end int32, elasticClient *elasticClientAlias) {
	var wg sync.WaitGroup
	sugar := zap.NewExample().Sugar()
	defer sugar.Sync()
	for height := from; height < end; height++ {
		dumpBlockTime := time.Now()
		block, err := btcClient.getBlock(height)
		if err != nil {
			log.Fatalln(err.Error())
		} else {
			wg.Add(2)
			// 这个地址交易数据比较明显，
			// 结合 https://blockchain.info/address/12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S 的交易数据测试验证同步逻辑 (该地址上 2009 年的交易数据)
			go elasticClient.BTCRollBackAndSyncTx(from, height, block, &wg)
			go elasticClient.BTCRollBackAndSyncBlock(from, height, block, &wg)
		}
		wg.Wait()
		sugar.Info("Dump block ", block.Height, " ", block.Hash, " dumpBlockTimeElapsed ", time.Since(dumpBlockTime))
	}
}

func (client *elasticClientAlias) BTCRollBackAndSyncBlock(from, height int32, block *btcjson.GetBlockVerboseResult, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.Background()
	bodyParams := BTCBlockWithTxDetail(block)
	if height < (from + 5) {
		// https://github.com/olivere/elastic/blob/release-branch.v6/update_test.go
		// https://www.elastic.co/guide/en/elasticsearch/reference/5.2/docs-update.html
		DocParams := BTCBlockWithTxDetail(block)
		client.Update().Index("block").Type("block").Id(strconv.FormatInt(int64(height), 10)).
			Doc(DocParams).DocAsUpsert(true).Upsert(DocParams).DetectNoop(true).Refresh("true").Do(ctx)
		log.Warnln("Update block:", block.Height, block.Hash)
	} else {
		_, err := client.Index().Index("block").Type("block").Id(strconv.FormatInt(int64(height), 10)).BodyJson(bodyParams).Do(ctx)
		if err != nil {
			log.Fatalln("write doc error", err.Error())
		}
		client.Flush()
	}
}

func (client *elasticClientAlias) syncTx(ctx context.Context, block *btcjson.GetBlockVerboseResult) error {
	bulkRequest := client.Bulk()
	var (
		vinAddressWithAmountSlice         []*Balance
		voutAddressWithAmountSlice        []*Balance
		vinAddresses                      []interface{} // All addresses related with vins in a block
		voutAddresses                     []interface{} // All addresses related with vouts in a block
		vinBalancesWithIDs                []*BalanceWithID
		voutBalancesWithIDs               []*BalanceWithID
		UniqueVoutAddressesWithSumDeposit []*AddressWithAmount // 统计区块中所有 vout 涉及到去重后的 vout 地址及其对应的增加余额
		UniqueVinAddressesWithSumWithdraw []*AddressWithAmount // 统计区块中所有 vout 涉及到去重后的 vout 地址及其对应的增加余额
	)

	for _, tx := range block.Tx {
		var (
			voutAmount       decimal.Decimal
			vinAmount        decimal.Decimal
			fee              decimal.Decimal
			txTypeVinsField  []*AddressWithValueInTx
			txTypeVoutsField []*AddressWithValueInTx
		)

		for _, vout := range tx.Vout {
			//  bulk insert vouts
			newVout, err := newVoutFun(vout, tx.Vin, tx.Txid)
			if err != nil {
				continue
			}
			createdVout := elastic.NewBulkIndexRequest().Index("vout").Type("vout").Doc(newVout)
			bulkRequest.Add(createdVout)
			elastic.NewSliceQuery()

			// vout amount
			voutAmount = voutAmount.Add(decimal.NewFromFloat(vout.Value))

			// vouts field in tx type
			txTypeVoutsFieldTmp, voutAddressesTmp, voutAddressWithAmountSliceTmp := parseTxVout(vout)
			txTypeVoutsField = append(txTypeVoutsField, txTypeVoutsFieldTmp...)
			voutAddresses = append(voutAddresses, voutAddressesTmp...)
			voutAddressWithAmountSlice = append(voutAddressWithAmountSlice, voutAddressWithAmountSliceTmp...)
		}

		// get es vouts with id in elasticsearch by tx vins
		indexVins := indexedVinsFun(tx.Vin)
		voutWithIDs, err := client.QueryVoutWithVinsOrVouts(ctx, indexVins)
		if err != nil {
			log.Fatalln("sync tx error: vout not found", err.Error())
		}
		if len(voutWithIDs) <= 0 {
			continue
		}

		for _, voutWithID := range voutWithIDs {
			// vin amount
			vinAmount = vinAmount.Add(decimal.NewFromFloat(voutWithID.Vout.Value))
			// update vout type used field
			updateVoutUsedField := elastic.NewBulkUpdateRequest().Index("vout").Type("vout").Id(voutWithID.ID).
				Doc(map[string]interface{}{"used": voutUsed{Txid: tx.Txid, VinIndex: voutWithID.Vout.Voutindex}})
			bulkRequest.Add(updateVoutUsedField).Refresh("true")

			txTypeVinsFieldTmp, vinAddressesTmp, vinAddressWithAmountSliceTmp := parseESVout(voutWithID)
			txTypeVinsField = append(txTypeVinsField, txTypeVinsFieldTmp...)
			vinAddresses = append(vinAddresses, vinAddressesTmp...)
			vinAddressWithAmountSlice = append(vinAddressWithAmountSlice, vinAddressWithAmountSliceTmp...)
		}

		// caculate tx fee
		fee = vinAmount.Sub(voutAmount)
		if len(tx.Vin) == 1 && len(tx.Vin[0].Coinbase) != 0 && len(tx.Vin[0].Txid) == 0 || vinAmount.Equal(voutAmount) {
			fee = decimal.NewFromFloat(0)
		}

		// bulk insert tx docutment
		txBulk := esTxFun(tx.Txid, block.Hash, fee.String(), tx.Time, txTypeVinsField, txTypeVoutsField)
		createdTx := elastic.NewBulkIndexRequest().Index("tx").Type("tx").Doc(txBulk)
		bulkRequest.Add(createdTx)
	}

	// 统计块中所有交易 vin 涉及到的地址及其对应的余额 (balance type)
	UniqueVinAddressesWithSumWithdraw = calculateUniqueAddressWithSumForVinOrVout(vinAddresses, vinAddressWithAmountSlice)
	bulkQueryVinBalance, err := client.BulkQueryBalance(ctx, vinAddresses...)
	if err != nil {
		log.Fatalln(err.Error())
	}
	vinBalancesWithIDs = bulkQueryVinBalance

	// 判断去重后的区块中所有交易的 vin 涉及到的地址数量是否与从 es 数据库中查询得到的 vinBalancesWithIDs 数量是否一直
	// 不一致则说明 balance type 中存在某个地址重复数据，此时应重新同步数据 TODO
	UniqueVinAddresses := removeDuplicatesForSlice(vinAddresses...)
	if len(UniqueVinAddresses) != len(vinBalancesWithIDs) {
		log.Fatalln("There are duplicate records in balances type")
	}

	bulkUpdateVinBalanceRequest := client.Bulk()
	// update(sub)  balances related to vins addresses
	// len(vinAddressWithSumWithdraw) == len(vinBalancesWithIDs)
	for _, vinAddressWithSumWithdraw := range UniqueVinAddressesWithSumWithdraw {
		for _, vinBalanceWithID := range vinBalancesWithIDs {
			if vinAddressWithSumWithdraw.Address == vinBalanceWithID.Balance.Address {
				balance := decimal.NewFromFloat(vinBalanceWithID.Balance.Amount).Sub(vinAddressWithSumWithdraw.Amount)
				amount, _ := balance.Float64()
				updateVinBalcne := elastic.NewBulkUpdateRequest().Index("balance").Type("balance").Id(vinBalanceWithID.ID).
					Doc(map[string]interface{}{"amount": amount})
				bulkUpdateVinBalanceRequest.Add(updateVinBalcne).Refresh("true")
				break
			}
		}
	}
	// vin 涉及到的地址余额必须在 vout 涉及到的地址余额之前更新，原因如下：
	// 但一笔交易中的 vins 里面的地址同时出现在 vout 中（就是常见的找零），那么对于这个地址而言，必须先减去 vin 的余额，再加上 vout 的余额
	if bulkUpdateVinBalanceRequest.NumberOfActions() != 0 {
		bulkUpdateVinBalanceResp, e := bulkUpdateVinBalanceRequest.Refresh("true").Do(ctx)
		if e != nil {
			log.Fatalln(err.Error())
		}
		bulkUpdateVinBalanceResp.Updated()
	}

	// 统计区块中所有 vout 涉及到去重后的 vout 地址及其对应的增加余额
	UniqueVoutAddressesWithSumDeposit = calculateUniqueAddressWithSumForVinOrVout(voutAddresses, voutAddressWithAmountSlice)
	bulkQueryVoutBalance, err := client.BulkQueryBalance(ctx, voutAddresses...)
	if err != nil {
		log.Fatalln(err.Error())
	}
	voutBalancesWithIDs = bulkQueryVoutBalance
	// update(add) or insert balances related to vouts addresses
	// len(voutAddressWithSumDeposit) >= len(voutBalanceWithID)
	for _, voutAddressWithSumDeposit := range UniqueVoutAddressesWithSumDeposit {
		var isNewBalance bool
		isNewBalance = true
		for _, voutBalanceWithID := range voutBalancesWithIDs {
			// update balance
			if voutAddressWithSumDeposit.Address == voutBalanceWithID.Balance.Address {
				balance := voutAddressWithSumDeposit.Amount.Add(decimal.NewFromFloat(voutBalanceWithID.Balance.Amount))
				amount, _ := balance.Float64()
				updateVoutBalcne := elastic.NewBulkUpdateRequest().Index("balance").Type("balance").Id(voutBalanceWithID.ID).
					Doc(map[string]interface{}{"amount": amount})
				bulkRequest.Add(updateVoutBalcne).Refresh("true")
				isNewBalance = false
				break
			}
		}

		// if voutAddressWithSumDeposit not exist in balance ES Type, insert a docutment
		if isNewBalance {
			amount, _ := voutAddressWithSumDeposit.Amount.Float64()
			newBalance := &Balance{
				Address: voutAddressWithSumDeposit.Address,
				Amount:  amount,
			}
			//  bulk insert balance
			insertBalance := elastic.NewBulkIndexRequest().Index("balance").Type("balance").Doc(newBalance)
			bulkRequest.Add(insertBalance).Refresh("true")
		}
	}

	bulkResp, err := bulkRequest.Refresh("true").Do(ctx)
	if err != nil {
		log.Fatalln(err.Error())
	}

	bulkResp.Created()
	bulkResp.Updated()
	bulkResp.Indexed()

	return errors.New("test error")
}
