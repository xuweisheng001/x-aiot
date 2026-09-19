SHELL := /bin/bash
SVCS := bootstrap-svc auth-svc conn-gate bridge pipeline deviceapi alarm-svc ota-svc cf001-svc \
        param-svc job-svc reco-job accessory-svc health-svc fleet-svc support-svc probe \
        device-simulator loadgen
export IOT_MQTT_URL ?= tcp://127.0.0.1:1883

.PHONY: dev down build test test-it vet keys run-% smoke loadtest-conn loadtest-storm loadtest-flood schema-td

dev:            ## 起依赖：emqx nats redis postgres tdengine
	docker compose -f deploy/docker-compose.yml up -d
	@echo "waiting for postgres/tdengine..."; sleep 8
	$(MAKE) schema-td
down:
	docker compose -f deploy/docker-compose.yml down -v
build:
	go build ./...
vet:
	go vet ./...
test:           ## 单测（无依赖，-race）
	go test -race ./...
test-it:        ## 集成测试（打真实依赖）
	IOT_IT=1 go test -race -count=1 ./...
keys:           ## 生成 CF001 开发密钥对（生产需分两对且签名私钥进 HSM）
	mkdir -p data/keys && openssl genrsa -out data/keys/dev.pem 2048 2>/dev/null && openssl rsa -in data/keys/dev.pem -pubout -out data/keys/dev.pub 2>/dev/null && echo "keys -> data/keys/"
schema-td:      ## 幂等初始化 TDengine 超级表与降采样流
	go run ./cmd/pipeline -init-schema
run-%:          ## make run-pipeline
	go run ./cmd/$*
smoke:          ## 50 台模拟器 + 一条火焰事件的端到端冒烟（需先各服务已起）
	bash scripts/smoke.sh
loadtest-conn:
	go run ./cmd/loadgen conn -n 5000 -rate 500 -target 127.0.0.1:1884 -out docs/loadtest/conn.csv
loadtest-storm:
	go run ./cmd/loadgen storm -n 3000 -target 127.0.0.1:1884 -out docs/loadtest/storm.csv
loadtest-flood:
	go run ./cmd/loadgen flood -pre 30000 -out docs/loadtest/flood.csv
help:
	@grep -E '^[a-zA-Z_%-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-16s %s\n",$$1,$$2}'
