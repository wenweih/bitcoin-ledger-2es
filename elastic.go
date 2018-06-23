package main

import (
	"github.com/olivere/elastic"
)

type elasticClientAlias struct {
	*elastic.Client
}

func (conf configure) elasticClient() (*elasticClientAlias, error) {
	client, err := elastic.NewClient(elastic.SetURL(conf.ElasticURL),
		elastic.SetSniff(conf.ElasticSniff))
	if err != nil {
		return nil, err
	}
	elasticClient := elasticClientAlias{client}
	return &elasticClient, nil
}
