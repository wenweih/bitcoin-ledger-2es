package main

import (
	"context"
	"strings"

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
	// btcClient.BTCSync(ctx, index, int32(1), hightest, *elasticClient)
}
