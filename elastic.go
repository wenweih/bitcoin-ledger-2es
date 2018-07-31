package main

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/olivere/elastic"
	"github.com/shopspring/decimal"
)

type elasticClientAlias struct {
	*elastic.Client
}

func (conf configure) elasticClient() (*elasticClientAlias, error) {
	client, err := elastic.NewClient(
		elastic.SetURL(conf.ElasticURL),
		elastic.SetSniff(conf.ElasticSniff))
	if err != nil {
		return nil, err
	}
	elasticClient := elasticClientAlias{client}
	return &elasticClient, nil
}

func (client *elasticClientAlias) createIndices() {
	ctx := context.Background()
	for _, index := range []string{"block", "tx", "vout", "balance"} {
		var mapping string
		switch index {
		case "block":
			mapping = blockMapping
		case "tx":
			mapping = txMapping
		case "vout":
			mapping = voutMapping
		case "balance":
			mapping = balanceMapping
		}
		result, err := client.CreateIndex(index).BodyString(mapping).Do(ctx)
		if err != nil {
			continue
		}
		if result.Acknowledged {
			sugar.Info(strings.Join([]string{"Create index:", result.Index}, ""))
		}
	}
}

func (client *elasticClientAlias) MaxAgg(field, index, typeName string) (*float64, error) {
	ctx := context.Background()
	hightestAgg := elastic.NewMaxAggregation().Field(field)
	aggKey := strings.Join([]string{"max", field}, "_")
	// Get Query params https://github.com/olivere/elastic/blob/release-branch.v6/search_aggs_metrics_max_test.go
	// https://www.elastic.co/guide/en/elasticsearch/reference/6.2/search-aggregations-metrics-max-aggregation.html
	searchResult, err := client.Search().
		Index(index).Type(typeName).
		Query(elastic.NewMatchAllQuery()).
		Aggregation(aggKey, hightestAgg).
		Do(ctx)

	if err != nil {
		return nil, err
	}
	maxAggRes, found := searchResult.Aggregations.Max(aggKey)
	if !found || maxAggRes.Value == nil {
		return nil, errors.New("query max agg error")
	}
	return maxAggRes.Value, nil
}

func (client *elasticClientAlias) QueryVoutWithVinsOrVoutsUnlimitSize(ctx context.Context, IndexUTXOs []IndexUTXO) ([]*VoutWithID, error) {
	var (
		voutWithIDs  []*VoutWithID
		IndexUTXOTmp []IndexUTXO
	)
	for len(IndexUTXOs) >= 500 {
		IndexUTXOTmp, IndexUTXOs = IndexUTXOs[:500], IndexUTXOs[500:]
		voutWithIDsTmp, err := client.QueryVoutWithVinsOrVouts(ctx, IndexUTXOTmp)
		if err != nil {
			sugar.Fatal("Chunks IndexUTXOs error")
		}
		voutWithIDs = append(voutWithIDs, voutWithIDsTmp...)
	}

	if len(IndexUTXOs) > 0 {
		voutWithIDsTmp, err := client.QueryVoutWithVinsOrVouts(ctx, IndexUTXOs)
		if err != nil {
			sugar.Fatalf("Chunks IndexUTXOs error")
		}
		voutWithIDs = append(voutWithIDs, voutWithIDsTmp...)
	}
	return voutWithIDs, nil
}

func (client *elasticClientAlias) QueryVoutWithVinsOrVouts(ctx context.Context, IndexUTXOs []IndexUTXO) ([]*VoutWithID, error) {
	q := elastic.NewBoolQuery()
	for _, vin := range IndexUTXOs {
		qnestedBool := elastic.NewBoolQuery()
		qnestedBool.Must(elastic.NewTermQuery("txidbelongto", vin.Txid), elastic.NewTermQuery("voutindex", vin.Index))
		q.Should(qnestedBool)
	}
	searchResult, err := client.Search().Index("vout").Type("vout").Size(len(IndexUTXOs)).Query(q).Do(ctx)
	if err != nil {
		return nil, errors.New(strings.Join([]string{"query vouts error:", err.Error()}, ""))
	}

	var voutWithIDs []*VoutWithID
	for _, vout := range searchResult.Hits.Hits {
		newVout := new(VoutStream)
		if err := json.Unmarshal(*vout.Source, newVout); err != nil {
			sugar.Fatalf(strings.Join([]string{"query vouts error: unmarshal json ", err.Error()}, " "))
		}
		voutWithIDs = append(voutWithIDs, &VoutWithID{vout.Id, newVout})
	}
	return voutWithIDs, nil
}

func (client *elasticClientAlias) QueryEsBlockByHeight(ctx context.Context, height int32) (*btcjson.GetBlockVerboseResult, error) {
	blockHeightStr := strconv.FormatInt(int64(height), 10)
	res, err := client.Get().Index("block").Type("block").Id(blockHeightStr).Refresh("true").Do(ctx)
	if err != nil {
		return nil, err
	}
	if !res.Found {
		return nil, errors.New(strings.Join([]string{"block:", blockHeightStr, "not fount in es when update txstream"}, ""))
	}
	NewBlock := new(btcjson.GetBlockVerboseResult)
	err = json.Unmarshal(*res.Source, NewBlock)
	if err != nil {
		return nil, err
	}
	return NewBlock, nil
}

// FindVoutsByUsedFieldAndBelongTxID 根据 vins 的 used object 和所在交易 ID 在 voutStream type 中查找 vouts ids
func (client *elasticClientAlias) QueryVoutsByUsedFieldAndBelongTxID(ctx context.Context, vins []btcjson.Vin, txBelongto string) ([]*VoutWithID, error) {
	if len(vins) == 1 && len(vins[0].Coinbase) != 0 && len(vins[0].Txid) == 0 {
		return nil, errors.New("coinbase tx, vin is new and not exist in es vout Type")
	}
	var esVoutIDS []string

	q := elastic.NewBoolQuery()
	for _, vin := range vins {
		bq := elastic.NewBoolQuery()
		bq = bq.Must(elastic.NewTermQuery("txidbelongto", vin.Txid))  // voutStream 所在的交易 ID 属于 vin 的 TxID 字段
		bq = bq.Must(elastic.NewTermQuery("used.txid", txBelongto))   // vin 所在的交易 ID 属于 voutStream used object 中的 txid 字段
		bq = bq.Must(elastic.NewTermQuery("used.vinindex", vin.Vout)) // vin 所在的交易输入索引属于 voutStream used object 中的 vinindex 字段
		q.Should(bq)
	}

	searchResult, err := client.Search().Index("vout").Type("vout").Size(len(vins)).Query(q).Do(ctx)
	if err != nil {
		return nil, err
	}
	if len(searchResult.Hits.Hits) < 1 {
		return nil, errors.New("vout not found by the condition")
	}

	var voutWithIDs []*VoutWithID
	for _, rawHit := range searchResult.Hits.Hits {
		newVout := new(VoutStream)
		if err := json.Unmarshal(*rawHit.Source, newVout); err != nil {
			sugar.Fatalf(err.Error())
		}
		esVoutIDS = append(esVoutIDS, rawHit.Id)
		voutWithIDs = append(voutWithIDs, &VoutWithID{rawHit.Id, newVout})
	}
	return voutWithIDs, nil
}

func (client *elasticClientAlias) RollbackTxVoutBalanceTypeByBlockHeight(ctx context.Context, height int32) error {
	bulkRequest := client.Bulk()
	var (
		vinAddresses                      []interface{} // All addresses related with vins in a block
		voutAddresses                     []interface{} // All addresses related with vouts in a block
		vinAddressWithAmountSlice         []*Balance
		voutAddressWithAmountSlice        []*Balance
		UniqueVinAddressesWithSumWithdraw []*AddressWithAmount // 统计区块中所有 vout 涉及到去重后的 vout 地址及其对应的增加余额
		UniqueVoutAddressesWithSumDeposit []*AddressWithAmount // 统计区块中所有 vout 涉及到去重后的 vout 地址及其对应的增加余额
		vinBalancesWithIDs                []*BalanceWithID
		voutBalancesWithIDs               []*BalanceWithID
	)

	NewBlock, err := client.QueryEsBlockByHeight(ctx, height)
	if err != nil {
		return err
	}

	// rollback: delete txs in es by block hash
	if e := client.DeleteEsTxsByBlockHash(ctx, NewBlock.Hash); e != nil {
		return e
	}

	for _, tx := range NewBlock.Tx {
		// es 中 vout 的 used 字段为 nil 涉及到的 vins 地址余额不用回滚
		voutWithIDSliceForVins, _ := client.QueryVoutsByUsedFieldAndBelongTxID(ctx, tx.Vin, tx.Txid)

		// 如果 len(voutWithIDSliceForVins) 为 0 ，则表面已经回滚过了，
		for _, voutWithID := range voutWithIDSliceForVins {
			// rollback: update vout's used to nil
			updateVoutUsedField := elastic.NewBulkUpdateRequest().Index("vout").Type("vout").Id(voutWithID.ID).
				Doc(map[string]interface{}{"used": nil})
			bulkRequest.Add(updateVoutUsedField).Refresh("true")

			_, vinAddressesTmp, vinAddressWithAmountSliceTmp := parseESVout(voutWithID)
			vinAddresses = append(vinAddresses, vinAddressesTmp...)
			vinAddressWithAmountSlice = append(vinAddressWithAmountSlice, vinAddressWithAmountSliceTmp...)
		}

		// get es vouts with id in elasticsearch by tx vouts
		indexVouts := indexedVoutsFun(tx.Vout, tx.Txid)
		// 没有被删除的 vouts 涉及到的 vout 地址才需要回滚余额
		voutWithIDSliceForVouts, e := client.QueryVoutWithVinsOrVouts(ctx, indexVouts)
		if e != nil {
			sugar.Fatal(strings.Join([]string{"QueryVoutWithVinsOrVouts error: vout not found", e.Error()}, " "))
		}
		for _, voutWithID := range voutWithIDSliceForVouts {
			// rollback: delete vout
			deleteVout := elastic.NewBulkDeleteRequest().Index("vout").Type("vout").Id(voutWithID.ID)
			bulkRequest.Add(deleteVout)

			_, voutAddressesTmp, voutAddressWithAmountSliceTmp := parseESVout(voutWithID)
			voutAddresses = append(voutAddresses, voutAddressesTmp...)
			voutAddressWithAmountSlice = append(voutAddressWithAmountSlice, voutAddressWithAmountSliceTmp...)
		}
	}

	// 统计块中所有交易 vin 涉及到的地址及其对应的提现余额 (balance type)
	UniqueVinAddressesWithSumWithdraw = calculateUniqueAddressWithSumForVinOrVout(vinAddresses, vinAddressWithAmountSlice)
	bulkQueryVinBalance, err := client.BulkQueryBalance(ctx, vinAddresses...)
	if err != nil {
		sugar.Fatal(err.Error())
	}
	vinBalancesWithIDs = bulkQueryVinBalance

	// 统计块中所有交易 vin 涉及到的地址及其对应的提现余额 (balance type)
	UniqueVoutAddressesWithSumDeposit = calculateUniqueAddressWithSumForVinOrVout(voutAddresses, voutAddressWithAmountSlice)
	bulkQueryVoutBalance, err := client.BulkQueryBalance(ctx, voutAddresses...)
	if err != nil {
		sugar.Fatalf(err.Error())
	}
	voutBalancesWithIDs = bulkQueryVoutBalance

	// rollback: add to addresses related to vins addresses
	// 通过 vin 在 vout type 的 used 字段查出来(不为 nil)的地址余额才回滚
	bulkUpdateVinBalanceRequest := client.Bulk()
	// update(sub)  balances related to vins addresses
	// len(vinAddressWithSumWithdraw) == len(vinBalancesWithIDs)
	for _, vinAddressWithSumWithdraw := range UniqueVinAddressesWithSumWithdraw {
		for _, vinBalanceWithID := range vinBalancesWithIDs {
			if vinAddressWithSumWithdraw.Address == vinBalanceWithID.Balance.Address {
				balance := decimal.NewFromFloat(vinBalanceWithID.Balance.Amount).Add(vinAddressWithSumWithdraw.Amount)
				amount, _ := balance.Float64()
				updateVinBalance := elastic.NewBulkUpdateRequest().Index("balance").Type("balance").Id(vinBalanceWithID.ID).
					Doc(map[string]interface{}{"amount": amount})
				bulkUpdateVinBalanceRequest.Add(updateVinBalance).Refresh("true")
				break
			}
		}
	}
	if bulkUpdateVinBalanceRequest.NumberOfActions() != 0 {
		bulkUpdateVinBalanceResp, e := bulkUpdateVinBalanceRequest.Refresh("true").Do(ctx)
		if e != nil {
			sugar.Fatalf(err.Error())
		}
		bulkUpdateVinBalanceResp.Updated()
	}

	// update(sub) balances related to vouts addresses
	// len(voutAddressWithSumDeposit) >= len(voutBalanceWithID)
	// 没有被删除的 vouts 涉及到的 vout 地址才需要回滚余额
	for _, voutAddressWithSumDeposit := range UniqueVoutAddressesWithSumDeposit {
		for _, voutBalanceWithID := range voutBalancesWithIDs {
			if voutAddressWithSumDeposit.Address == voutBalanceWithID.Balance.Address {
				balance := decimal.NewFromFloat(voutBalanceWithID.Balance.Amount).Sub(voutAddressWithSumDeposit.Amount)
				amount, _ := balance.Float64()
				updateVinBalance := elastic.NewBulkUpdateRequest().Index("balance").Type("balance").Id(voutBalanceWithID.ID).
					Doc(map[string]interface{}{"amount": amount})
				bulkRequest.Add(updateVinBalance).Refresh("true")
				break
			}
		}
	}

	if bulkRequest.NumberOfActions() != 0 {
		bulkResp, err := bulkRequest.Refresh("true").Do(ctx)
		if err != nil {
			sugar.Fatal(err.Error())
		}
		bulkResp.Updated()
		bulkResp.Deleted()
		bulkResp.Indexed()
	}

	return nil
}

func (client *elasticClientAlias) DeleteEsTxsByBlockHash(ctx context.Context, blockHash string) error {
	q := elastic.NewTermQuery("blockhash", blockHash)
	if _, err := client.DeleteByQuery().Index("tx").Type("tx").Query(q).Refresh("true").Do(ctx); err != nil {
		return errors.New(strings.Join([]string{"Delete", blockHash, "'s all transactions from es tx type fail"}, ""))
	}
	return nil
}

// BulkQueryBalance query balances by address slice
func (client *elasticClientAlias) BulkQueryBalance(ctx context.Context, addresses ...interface{}) ([]*BalanceWithID, error) {
	var (
		balancesWithIDs []*BalanceWithID
		qAddresses      []interface{}
	)

	uniqueAddresses := removeDuplicatesForSlice(addresses...)
	for _, address := range uniqueAddresses {
		qAddresses = append(qAddresses, address)
	}

	q := elastic.NewTermsQuery("address", qAddresses...)
	searchResult, err := client.Search().Index("balance").Type("balance").Size(len(qAddresses)).Query(q).Do(ctx)
	if err != nil {
		return nil, errors.New(strings.Join([]string{"Get balances error:", err.Error()}, " "))
	}

	for _, balance := range searchResult.Hits.Hits {
		b := new(Balance)
		if err := json.Unmarshal(*balance.Source, b); err != nil {
			return nil, errors.New(strings.Join([]string{"unmarshal error:", err.Error()}, " "))
		}
		balancesWithIDs = append(balancesWithIDs, &BalanceWithID{balance.Id, *b})
	}
	return balancesWithIDs, nil
}

// 统计块中的所有 vout 涉及到去重后的所有地址对应充值额度
func calculateUniqueAddressWithSumForVinOrVout(addresses []interface{}, AddressWithAmountSlice []*Balance) []*AddressWithAmount {
	var UniqueAddressesWithSum []*AddressWithAmount
	UniqueAddresses := removeDuplicatesForSlice(addresses...)
	// 统计去重后涉及到的 vout 地址及其对应的增加余额
	for _, uAddress := range UniqueAddresses {
		sumDeposit := decimal.NewFromFloat(0)
		for _, addressWithAmount := range AddressWithAmountSlice {
			if uAddress == addressWithAmount.Address {
				sumDeposit = sumDeposit.Add(decimal.NewFromFloat(addressWithAmount.Amount))
			}
		}
		UniqueAddressesWithSum = append(UniqueAddressesWithSum, &AddressWithAmount{uAddress, sumDeposit})
	}
	return UniqueAddressesWithSum
}
