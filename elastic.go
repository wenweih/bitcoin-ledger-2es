package main

import (
	"context"
	"strings"

	"github.com/olivere/elastic"
	log "github.com/sirupsen/logrus"
)

type elasticClientAlias struct {
	*elastic.Client
}

const blockMapping = `
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "block": {
      "properties": {
        "hash": {
          "type": "keyword"
        },
        "strippedsize": {
          "type": "integer"
        },
        "size": {
          "type": "integer"
        },
        "weight": {
          "type": "integer"
        },
        "height": {
          "type": "integer"
        },
        "versionHex": {
          "type": "text"
        },
        "merkleroot": {
          "type": "text"
        },
        "tx": {
          "properties": {
            "hex": {
              "type": "text"
            },
            "txid": {
              "type": "text"
            },
            "hash": {
              "type": "text"
            },
            "version": {
              "type": "short"
            },
            "size": {
              "type": "integer"
            },
            "vsize": {
              "type": "integer"
            },
            "locktime": {
              "type": "long"
            },
            "vin": {
              "properties": {
                "txid": {
                  "type": "text"
                },
                "vout": {
                  "type": "short"
                },
                "scriptSig": {
                  "properties": {
                    "asm": {
                      "type": "text"
                    },
                    "hex": {
                      "type": "text"
                    }
                  }
                },
                "sequence": {
                  "type": "long"
                },
                "txinwitness": {
                  "type":"keyword"
                }
              }
            },
            "vout": {
              "properties": {
                "value": {
                  "type": "double"
                },
                "n": {
                  "type": "short"
                },
                "scriptPubKey": {
                  "properties": {
                    "asm": {
                      "type": "text"
                    },
                    "hex": {
                      "type": "text"
                    },
                    "reqSigs": {
                      "type": "short"
                    },
                    "type": {
                      "type": "text"
                    },
                    "addresses": {
                      "type":"keyword"
                    }
                  }
                }
              }
            }
          }
        },
        "time": {
          "type": "long"
        },
        "mediantime": {
          "type": "long"
        },
        "nonce": {
          "type": "long"
        },
        "bits": {
          "type": "text"
        },
        "difficulty": {
          "type": "double"
        },
        "chainwork": {
          "type": "text"
        },
        "previoushash": {
          "type": "text"
        },
        "nexthash": {
          "type": "text"
        }
      }
    }
  }
}`

const txMapping = `
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
		"tx": {
      "properties": {
        "txid": {
          "type": "text"
        },
        "fee": {
          "type": "double"
        },
				"blockhash": {
					"type": "text"
				},
        "vins": {
          "type": "nested",
          "properties": {
            "address": {
              "type": "text"
            },
            "value": {
              "type": "double"
            }
          }
        },
        "vouts": {
          "type": "nested",
          "properties": {
            "address": {
              "type": "text"
            },
            "value": {
              "type": "double"
            }
          }
        },
        "time": {
          "type": "long"
        }
      }
    }
  }
}`

const voutMapping = `
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
		"vout": {
      "properties": {
        "txidbelongto": {
          "type": "text"
        },
        "value": {
          "type": "double"
        },
        "voutindex": {
          "type": "short"
        },
        "coinbase": {
          "type": "boolean"
        },
        "addresses": {
          "type":"keyword"
        },
				"time": {
					"type": "long"
				},
        "used": {
          "type":"object"
        }
      }
    }
  }
}`

const balanceMapping = `
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
		"balance": {
			"properties": {
				"address": {
					"type":"keyword"
				},
				"amount": {
					"type": "double"
				}
			}
		}
  }
}`

func (conf configure) elasticClient() (*elasticClientAlias, error) {
	client, err := elastic.NewClient(elastic.SetURL(conf.ElasticURL),
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
			log.Infoln(strings.Join([]string{"Create index:", result.Index}, ""))
		}
	}
}
