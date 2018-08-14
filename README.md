## Parse Bitcoin Ledger To Elasticsearch
Dump Bitcoin Mainnet ledger to elasticsearch. constructed specify format so we can query balance and utxo, the main propose of the repo is for wallet or block explorer
### Install
Environment require:
- Golang (compile)
- Dep (package dependency)
- Elasticsearch (database)

Before clone the repo, I wanna let claim that there is a bug I have verified the [btcd](https://github.com/btcsuite/btcd), an alternative full node bitcoin implementation written in Go. See the detail: [[RPC] getblock command has been changed](https://github.com/btcsuite/btcd/issues/1096), and I have given a solution how to fixed the problem

```bash
go get -u github.com/wenweih/btc-chaindata-2es
cd $GOPATH/src/github.com/wenweih/btc-chaindata-2es
dep ensure -v -update
```
After ```dep ensure -v -update``` to load the repo dependency, you should modify btcd package in ```vendor``` fold flowing by [修复开源项目 btcd RPC 实现比特币获取区块的问题](https://huangwenwei.com/blogs/fix-verbocity-in-getblock-command-for-btcd).

cross compile, such as for my Ubuntu Server:
```bash
GOARCH=amd64 GOOS=linux go build
```
### Usage
Because of btc-chaindata-2es service interacts with bitcoind by RPC and Elasticsearch by HTTP protocol, to avoid network request delay, I hightly recommend you run the three services (btc-chaindata-2ex, bitcoind and elasticsearch) in the same server.

Copy configure to ~/ directory
```bash
cat ~/btc-chaindata-2es.yml
btc_host: "127.0.0.1"
btc_port: "8791"
btc_usr: "usertest"
btc_pass: "passtest"
btc_http_mode: true
btc_disable_tls: true
elastic_url: "http://127.0.0.1:9200"
elastic_sniff: false
```

Start the service:
```
nohup ~/btc-chaindata-2es sync > /tmp/btc-chaindata-2es.log 2>&1 &
```
