package main

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type configure struct {
	BitcoinHost       string
	BitcoinPort       string
	BitcoinUser       string
	BitcoinPass       string
	BitcoinhttpMode   bool
	BitcoinDisableTLS bool
	ElasticURL        string
	ElasticSniff      bool
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "bitcoin-service",
	Short: "Bitcoin middleware for application",
}

var accountCmd = &cobra.Command{
	Use:   "genaccount",
	Short: "Generate Bitcoin account",
	Run: func(cmd *cobra.Command, args []string) {
	},
}

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Parse bitcoin chaindata to elasticsearch",
	Run: func(cmd *cobra.Command, args []string) {
		esClient, err := config.elasticClient()
		if err != nil {
			sugar.Fatal("es client error: ", err.Error())
		}

		esClient.createIndices()

		c := config.bitcoinClient()
		btcClient := bitcoinClientAlias{c}

		for {
			isContinue := esClient.Sync(btcClient)
			if !isContinue {
				sugar.Error("break syncing")
				break
			}
			esClient.Flush()
		}
	},
}

// Execute 命令行入口
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		sugar.Fatal("Command execute error: ", err.Error())
	}
}

func init() {
	sugar = zap.NewExample().Sugar()
	defer sugar.Sync()
	config = new(configure)
	config.InitConfig()
	rootCmd.AddCommand(syncCmd)
}

func (conf *configure) InitConfig() {
	viper.SetConfigType("yaml")
	viper.AddConfigPath(HomeDir())
	viper.SetConfigName("btc-chaindata-2es")
	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	err := viper.ReadInConfig()
	if err == nil {
		sugar.Info("Using Configure file:", viper.ConfigFileUsed())
	} else {
		sugar.Fatal("Error: bitcoin.yml not found in:", HomeDir())
	}

	for key, value := range viper.AllSettings() {
		switch key {
		case "btc_host":
			conf.BitcoinHost = value.(string)
		case "btc_port":
			conf.BitcoinPort = value.(string)
		case "btc_usr":
			conf.BitcoinUser = value.(string)
		case "btc_pass":
			conf.BitcoinPass = value.(string)
		case "btc_http_mode":
			conf.BitcoinhttpMode = value.(bool)
		case "btc_disable_tls":
			conf.BitcoinDisableTLS = value.(bool)
		case "elastic_url":
			conf.ElasticURL = value.(string)
		case "elastic_sniff":
			conf.ElasticSniff = value.(bool)

		}
	}
}
