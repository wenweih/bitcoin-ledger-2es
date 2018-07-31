package main

import (
	"go.uber.org/zap"
)

var (
	config *configure
	sugar  *zap.SugaredLogger
)

func main() {
	Execute()
}
