SHELL := /bin/sh
GO ?= go
BIN := bin

.PHONY: help build test demo verify clean

help:
	@echo "make build    编译 logforge 与 mocklog"
	@echo "make test     运行单元测试"
	@echo "make demo     生成日志并跑一次采集（约 10 秒）"
	@echo "make verify   断言输出与计数"
	@echo "make clean    清理产物与临时目录"

build:
	mkdir -p $(BIN)
	$(GO) build -o $(BIN)/logforge ./cmd/logforge
	$(GO) build -o $(BIN)/mocklog ./cmd/mocklog

test:
	$(GO) test ./... -count=1

demo: build
	sh scripts/demo.sh

verify: build
	sh scripts/verify.sh

clean:
	rm -rf $(BIN) /tmp/logforge
