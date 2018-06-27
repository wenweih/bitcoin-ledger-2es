package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestElasticClient(t *testing.T) {
	client, err := config.elasticClient()
	assert.Nil(t, err)
	assert.True(t, client.IsRunning())
}
