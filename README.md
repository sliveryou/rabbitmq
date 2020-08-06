# rabbitmq

[![Github License](https://img.shields.io/github/license/sliveryou/rabbitmq.svg?style=flat)](https://github.com/sliveryou/rabbitmq/blob/master/LICENSE)
[![Go Doc](https://godoc.org/github.com/sliveryou/rabbitmq?status.svg)](https://pkg.go.dev/github.com/sliveryou/rabbitmq)
[![Go Report](https://goreportcard.com/badge/github.com/sliveryou/rabbitmq)](https://goreportcard.com/report/github.com/sliveryou/rabbitmq)
[![Github Latest Release](https://img.shields.io/github/release/sliveryou/rabbitmq.svg?style=flat)](https://github.com/sliveryou/rabbitmq/releases/latest)
[![Github Latest Tag](https://img.shields.io/github/tag/sliveryou/rabbitmq.svg?style=flat)](https://github.com/sliveryou/arabbitmq/tags)
[![Github Stars](https://img.shields.io/github/stars/sliveryou/rabbitmq.svg?style=flat)](https://github.com/sliveryou/rabbitmq/stargazers)

Friendly RabbitMQ Wrapper.

## Installation

1. Create rabbitmq docker container by using:
```sh
$ docker run --name rabbitmq --hostname rabbitmq-test-node-1 -p 15672:15672 -p 5672:5672 -e RABBITMQ_DEFAULT_USER=root -e RABBITMQ_DEFAULT_PASS=123123 -d rabbitmq:3.8.5-management
```

2. Download rabbitmq package by using:
```sh
$ go get github.com/sliveryou/rabbitmq
```