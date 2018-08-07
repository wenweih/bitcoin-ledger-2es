package main

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/olivere/elastic"
	"github.com/shopspring/decimal"

	"github.com/btcsuite/btcd/btcjson"
)

// Sync dump bitcoin chaindata to es
func (esClient *elasticClientAlias) Sync(btcClient bitcoinClientAlias) bool {
	info, err := btcClient.GetBlockChainInfo()
	if err != nil {
		sugar.Fatal("Get info error: ", err.Error())
	}

	var DBCurrentHeight float64
	agg, err := esClient.MaxAgg("height", "block", "block")
	if err != nil {
		if err.Error() == "query max agg error" {
			btcClient.ReSetSync(info.Headers, esClient)
			return true
		}
		sugar.Warn(strings.Join([]string{"Query max aggration error:", err.Error()}, " "))
		return false
	}
	DBCurrentHeight = *agg

	heightGap := info.Headers - int32(DBCurrentHeight)
	switch {
	case heightGap > 0:
		esClient.RollbacBlocks(DBCurrentHeight, 5, btcClient)
	case heightGap == 0:
		esBestBlock, err := esClient.QueryEsBlockByHeight(context.TODO(), info.Headers)
		if err != nil {
			sugar.Fatal("Can't query best block in es")
		}

		nodeblock, err := btcClient.getBlock(info.Headers)
		if err != nil {
			sugar.Fatal("Can't query block from bitcoind")
		}

		if esBestBlock.Hash != nodeblock.Hash {
			esClient.RollbacBlocks(DBCurrentHeight, 5, btcClient)
		}
	case heightGap < 0:
		sugar.Fatal("bitcoind best height less than es best, something wrong")
	}
	return true
}

func (esClient *elasticClientAlias) RollbacBlocks(from float64, size int, btcClient bitcoinClientAlias) {
	syncIndex := strconv.FormatFloat(from-float64(size), 'f', -1, 64)
	SyncBeginRecord, err := esClient.Get().Index("block").Type("block").Id(syncIndex).Do(context.Background())
	if err != nil {
		sugar.Fatal("Query SyncBeginRecord error")
	}

	info, err := btcClient.GetBlockChainInfo()
	if err != nil {
		sugar.Fatal("Get info error: ", err.Error())
	}

	if !SyncBeginRecord.Found {
		btcClient.ReSetSync(info.Headers, esClient)
	} else {
		// 数据库倒退 5 个块再同步
		btcClient.SyncConcurrency(int32(int(from)-size), info.Headers, esClient)
	}
}

func (btcClient *bitcoinClientAlias) SyncConcurrency(from, end int32, elasticClient *elasticClientAlias) {
	var wg sync.WaitGroup
	for height := from; height < end; height++ {
		dumpBlockTime := time.Now()
		block, err := btcClient.getBlock(height)
		if err != nil {
			sugar.Fatal("Get block error: ", err.Error())
		} else {
			wg.Add(2)
			// 这个地址交易数据比较明显，
			// 结合 https://blockchain.info/address/12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S 的交易数据测试验证同步逻辑 (该地址上 2009 年的交易数据)
			go elasticClient.RollBackAndSyncTx(from, height, block, &wg)
			go elasticClient.RollBackAndSyncBlock(from, height, block, &wg)
		}
		wg.Wait()
		sugar.Info("Dump block ", block.Height, " ", block.Hash, " dumpBlockTimeElapsed ", time.Since(dumpBlockTime))
	}
}

func (esClient *elasticClientAlias) RollBackAndSyncTx(from, height int32, block *btcjson.GetBlockVerboseResult, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.Background()
	if height <= (from + 5) {
		esClient.RollbackTxVoutBalanceTypeByBlockHeight(ctx, height)
	}
	// client.BTCSyncTx(ctx, from, block)
	esClient.syncTx(ctx, block)
}

func (esClient *elasticClientAlias) RollBackAndSyncBlock(from, height int32, block *btcjson.GetBlockVerboseResult, wg *sync.WaitGroup) {
	ctx := context.Background()
	if height <= (from + 5) {
		_, err := esClient.Delete().Index("block").Type("block").Id(strconv.FormatInt(int64(height), 10)).Refresh("true").Do(ctx)
		if err != nil && err.Error() != "elastic: Error 404 (Not Found)" {
			sugar.Fatal("Delete block docutment error: ", err.Error())
		}

	}
	bodyParams := BTCBlockWithTxDetail(block)
	_, err := esClient.Index().Index("block").Type("block").Id(strconv.FormatInt(int64(height), 10)).BodyJson(bodyParams).Do(ctx)
	if err != nil {
		sugar.Fatal(strings.Join([]string{"Dump block docutment error", err.Error()}, " "))
	}
	defer wg.Done()
}

func (esClient *elasticClientAlias) syncTx(ctx context.Context, block *btcjson.GetBlockVerboseResult) {
	bulkRequest := esClient.Bulk()
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

	// TODO too slow, neet to optimization
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
			bulkRequest.Add(createdVout).Refresh("true")

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
		voutWithIDs, err := esClient.QueryVoutWithVinsOrVoutsUnlimitSize(ctx, indexVins)
		if err != nil {
			sugar.Fatal(strings.Join([]string{"sync tx error:", err.Error()}, " "))
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

		esFee, _ := fee.Float64()
		// bulk insert tx docutment
		txBulk := esTxFun(tx.Txid, block.Hash, esFee, tx.Time, txTypeVinsField, txTypeVoutsField)
		createdTx := elastic.NewBulkIndexRequest().Index("tx").Type("tx").Doc(txBulk)
		bulkRequest.Add(createdTx).Refresh("true")
	}

	// 统计块中所有交易 vin 涉及到的地址及其对应的余额 (balance type)
	UniqueVinAddressesWithSumWithdraw = calculateUniqueAddressWithSumForVinOrVout(vinAddresses, vinAddressWithAmountSlice)
	bulkQueryVinBalance, err := esClient.BulkQueryBalanceUnlimitSize(ctx, vinAddresses...)
	if err != nil {
		sugar.Fatal("Query balance related with vin error: ", err.Error())
	}
	vinBalancesWithIDs = bulkQueryVinBalance

	bulkUpdateVinBalanceRequest := esClient.Bulk()
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
			sugar.Fatal("update vin balance error: ", err.Error())
		}
		bulkUpdateVinBalanceResp.Updated()
	}

	// 统计区块中所有 vout 涉及到去重后的 vout 地址及其对应的增加余额
	UniqueVoutAddressesWithSumDeposit = calculateUniqueAddressWithSumForVinOrVout(voutAddresses, voutAddressWithAmountSlice)
	bulkQueryVoutBalance, err := esClient.BulkQueryBalanceUnlimitSize(ctx, voutAddresses...)
	if err != nil {
		sugar.Fatal("Query balance related with vouts address error: ", err.Error())
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
		sugar.Fatal("bulk request error: ", err.Error())
	}

	bulkResp.Created()
	bulkResp.Updated()
	bulkResp.Indexed()
}
