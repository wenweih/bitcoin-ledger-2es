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
		// elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)),
		// elastic.SetInfoLog(log.New(os.Stdout, "", log.LstdFlags)),
		elastic.SetSniff(conf.ElasticSniff))
	if err != nil {
		return nil, err
	}
	elasticClient := elasticClientAlias{client}
	return &elasticClient, nil
}

func (esClient *elasticClientAlias) createIndices() {
	ctx := context.Background()
	for _, index := range []string{"block", "tx", "vout", "balance", "balancejournal"} {
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
		case "balancejournal":
			mapping = balanceJournalMapping
		}
		result, err := esClient.CreateIndex(index).BodyString(mapping).Do(ctx)
		if err != nil {
			continue
		}
		if result.Acknowledged {
			sugar.Info(strings.Join([]string{"Create index:", result.Index}, ""))
		}
	}
}

func (esClient *elasticClientAlias) MaxAgg(field, index, typeName string) (*float64, error) {
	ctx := context.Background()
	hightestAgg := elastic.NewMaxAggregation().Field(field)
	aggKey := strings.Join([]string{"max", field}, "_")
	// Get Query params https://github.com/olivere/elastic/blob/release-branch.v6/search_aggs_metrics_max_test.go
	// https://www.elastic.co/guide/en/elasticsearch/reference/6.2/search-aggregations-metrics-max-aggregation.html
	searchResult, err := esClient.Search().
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

func (esClient *elasticClientAlias) QueryVoutWithVinsOrVoutsUnlimitSize(ctx context.Context, IndexUTXOs []IndexUTXO) []VoutWithID {
	var (
		voutWithIDs  []VoutWithID
		IndexUTXOTmp []IndexUTXO
	)
	for len(IndexUTXOs) >= 500 {
		IndexUTXOTmp, IndexUTXOs = IndexUTXOs[:500], IndexUTXOs[500:]
		voutWithIDsTmp, err := esClient.QueryVoutWithVinsOrVouts(ctx, IndexUTXOTmp)
		if err != nil {
			sugar.Fatal("Chunks IndexUTXOs error", err.Error())
		}
		voutWithIDs = append(voutWithIDs, voutWithIDsTmp...)
	}
	if len(IndexUTXOs) > 0 {
		voutWithIDsTmp, err := esClient.QueryVoutWithVinsOrVouts(ctx, IndexUTXOs)
		if err != nil {
			sugar.Fatal("Chunks IndexUTXOs error", err.Error())
		}
		voutWithIDs = append(voutWithIDs, voutWithIDsTmp...)
	}
	return voutWithIDs
}

func (esClient *elasticClientAlias) QueryVoutWithVinsOrVouts(ctx context.Context, IndexUTXOs []IndexUTXO) ([]VoutWithID, error) {
	q := elastic.NewBoolQuery()
	for _, vin := range IndexUTXOs {
		bq := elastic.NewBoolQuery()
		bq.Must(elastic.NewTermQuery("txidbelongto", vin.Txid))
		bq.Must(elastic.NewTermQuery("voutindex", vin.Index))
		q.Should(bq)
	}
	searchResult, err := esClient.Search().Index("vout").Type("vout").Size(len(IndexUTXOs)).Query(q).Do(ctx)
	if err != nil {
		return nil, errors.New(strings.Join([]string{"query vouts error:", err.Error()}, ""))
	}

	var voutWithIDs []VoutWithID
	for _, vout := range searchResult.Hits.Hits {
		newVout := new(VoutStream)
		if err := json.Unmarshal(*vout.Source, newVout); err != nil {
			sugar.Fatalf(strings.Join([]string{"query vouts error: unmarshal json ", err.Error()}, " "))
		}
		voutWithIDs = append(voutWithIDs, VoutWithID{vout.Id, newVout})
	}
	return voutWithIDs, nil
}

func (esClient *elasticClientAlias) QueryEsBlockByHeight(ctx context.Context, height int32) (*btcjson.GetBlockVerboseResult, error) {
	blockHeightStr := strconv.FormatInt(int64(height), 10)
	res, err := esClient.Get().Index("block").Type("block").Id(blockHeightStr).Refresh("true").Do(ctx)
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
func (esClient *elasticClientAlias) QueryVoutsByUsedFieldAndBelongTxID(ctx context.Context, vins []btcjson.Vin, txBelongto string) ([]VoutWithID, error) {
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

	searchResult, err := esClient.Search().Index("vout").Type("vout").Size(len(vins)).Query(q).Do(ctx)
	if err != nil {
		return nil, err
	}
	if len(searchResult.Hits.Hits) < 1 {
		return nil, errors.New("vout not found by the condition")
	}

	var voutWithIDs []VoutWithID
	for _, rawHit := range searchResult.Hits.Hits {
		newVout := new(VoutStream)
		if err := json.Unmarshal(*rawHit.Source, newVout); err != nil {
			sugar.Fatal("rallback: unmarshal es vout error", err.Error())
		}
		esVoutIDS = append(esVoutIDS, rawHit.Id)
		voutWithIDs = append(voutWithIDs, VoutWithID{rawHit.Id, newVout})
	}
	return voutWithIDs, nil
}

func (esClient *elasticClientAlias) DeleteEsTxsByBlockHash(ctx context.Context, blockHash string) error {
	q := elastic.NewTermQuery("blockhash", blockHash)
	if _, err := esClient.DeleteByQuery().Index("tx").Type("tx").Query(q).Refresh("true").Do(ctx); err != nil {
		return errors.New(strings.Join([]string{"Delete", blockHash, "'s all transactions from es tx type fail"}, ""))
	}
	return nil
}

func (esClient *elasticClientAlias) BulkInsertBalanceJournal(ctx context.Context, balancesWithID []AddressWithAmountAndTxid, ope string) {
	p, err := esClient.BulkProcessor().Name("BulkInsertBalanceJournal").Workers(5).BulkActions(40000).Do(ctx)
	if err != nil {
		sugar.Fatal("es BulkProcessor error: ", err.Error())
	}

	for _, balanceID := range balancesWithID {
		newBalanceJournal := newBalanceJournalFun(balanceID.Address, ope, balanceID.Txid, balanceID.Amount)
		insertBalanceJournal := elastic.NewBulkIndexRequest().Index("balancejournal").Type("balancejournal").Doc(newBalanceJournal)
		p.Add(insertBalanceJournal)
	}
	defer p.Close()
}

// BulkQueryBalanceUnlimitSize fixed query more than 1k
func (esClient *elasticClientAlias) BulkQueryBalanceUnlimitSize(ctx context.Context, addresses ...interface{}) ([]*BalanceWithID, error) {
	uniquAddresses := removeDuplicatesForSlice(addresses...)
	var uniqueAddressesI []interface{}
	for _, uniquAddressI := range uniquAddresses {
		uniqueAddressesI = append(uniqueAddressesI, uniquAddressI)
	}

	var (
		balanceWithIDs []*BalanceWithID
		addressesTmp   []interface{}
	)

	for len(uniqueAddressesI) >= 500 {
		addressesTmp, uniqueAddressesI = uniqueAddressesI[:500], uniqueAddressesI[500:]
		balanceWithIDsTmp, err := esClient.BulkQueryBalance(ctx, addressesTmp...)
		if err != nil {
			sugar.Fatal("Chunks addresses error")
		}
		balanceWithIDs = append(balanceWithIDs, balanceWithIDsTmp...)
	}
	if len(uniqueAddressesI) > 0 {
		balanceWithIDsTmp, err := esClient.BulkQueryBalance(ctx, uniqueAddressesI...)
		if err != nil {
			sugar.Fatal("Chunks addresses error")
		}
		balanceWithIDs = append(balanceWithIDs, balanceWithIDsTmp...)
	}
	return balanceWithIDs, nil
}

// BulkQueryBalance query balances by address slice
func (esClient *elasticClientAlias) BulkQueryBalance(ctx context.Context, addresses ...interface{}) ([]*BalanceWithID, error) {
	var (
		balancesWithIDs []*BalanceWithID
		qAddresses      []interface{}
	)

	uniqueAddresses := removeDuplicatesForSlice(addresses...)
	for _, address := range uniqueAddresses {
		qAddresses = append(qAddresses, address)
	}

	q := elastic.NewTermsQuery("address", qAddresses...)
	searchResult, err := esClient.Search().Index("balance").Type("balance").Size(len(qAddresses)).Query(q).Do(ctx)
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
func calculateUniqueAddressWithSumForVinOrVout(addresses []interface{}, AddressWithAmountSlice []Balance) []*AddressWithAmount {
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
