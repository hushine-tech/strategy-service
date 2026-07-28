PYTHON?=$(shell if [ -x .venv/bin/python ]; then echo .venv/bin/python; elif [ -x /opt/anaconda3/bin/python3 ]; then echo /opt/anaconda3/bin/python3; elif command -v python3 >/dev/null 2>&1; then command -v python3; elif command -v python >/dev/null 2>&1; then command -v python; fi)
UV?=$(shell if command -v uv >/dev/null 2>&1; then command -v uv; elif [ -n "$$HOME" ] && [ -x "$$HOME/.local/bin/uv" ]; then printf '%s\n' "$$HOME/.local/bin/uv"; else printf '%s\n' uv; fi)
export UV
PYTHONPATH_VAL=.:./strategy-library
CONFIG?=./config.yaml
PID_FILE=.run.pid
DEV_NO_PROXY_HOSTS ?= 127.0.0.1,localhost,::1,192.168.88.10
DEV_NO_PROXY := env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY -u all_proxy -u ALL_PROXY NO_PROXY=$(DEV_NO_PROXY_HOSTS),$${NO_PROXY} no_proxy=$(DEV_NO_PROXY_HOSTS),$${no_proxy}
VERSION?=dev

.PHONY: build build-release dependency-contract dev start stop clean test test-scripts runtime-images runtime-images-verify runtime-images-verify-dev

dependency-contract:
	"$(UV)" sync --python 3.13 --frozen --extra dev
	PYTHONPATH=../strategy-library "$(UV)" run --frozen \
		python ../strategy-library/scripts/check_runtime_dependency_contract.py \
		--service-project pyproject.toml --service-lock uv.lock \
		--installed-python strategy-service=.venv/bin/python \
		--installed-python-version strategy-service=3.13 \
		--json

test:
	PYTHONPATH=$(PYTHONPATH_VAL) $(UV) run --extra dev pytest tests/ -q
	go test ./...
	$(MAKE) test-scripts

test-scripts:
	bash scripts/start-bare-runtime-debugpy.test.sh
	bash scripts/runtime-agent-platform.test.sh

build:
	go build -o bin/runtime-agent ./cmd/runtime-agent

build-release:
	scripts/build-runtime-agent-release.sh --version $(VERSION)

runtime-images:
	./scripts/build_strategy_runtime.sh --all --allow-dirty dev

runtime-images-verify:
	./scripts/build_strategy_runtime.sh --all --no-cache --verify contract

runtime-images-verify-dev:
	./scripts/build_strategy_runtime.sh --all --no-cache --verify --allow-dirty contract

dev:
	$(DEV_NO_PROXY) PYTHONPATH=$(PYTHONPATH_VAL) go run ./cmd/runtime-agent --config $(CONFIG)

start:
	mkdir -p logs
	$(MAKE) build
	python3 -c 'import os, subprocess; env=os.environ.copy(); [env.pop(k, None) for k in ("http_proxy","https_proxy","HTTP_PROXY","HTTPS_PROXY","all_proxy","ALL_PROXY")]; env["NO_PROXY"]="$(DEV_NO_PROXY_HOSTS),"+env.get("NO_PROXY",""); env["no_proxy"]="$(DEV_NO_PROXY_HOSTS),"+env.get("no_proxy",""); env["PYTHONPATH"]="$(PYTHONPATH_VAL)"; out=open("logs/strategy-service.out","ab",buffering=0); p=subprocess.Popen(["./bin/runtime-agent","--config","$(CONFIG)"], stdout=out, stderr=subprocess.STDOUT, start_new_session=True, close_fds=True, env=env); open("$(PID_FILE)","w").write(str(p.pid)+"\n")'
	@echo "✓ strategy-service started (pid=$$(cat $(PID_FILE))), logs at strategy-service/logs/strategy-service.out"

stop:
	@if [ -f $(PID_FILE) ]; then kill $$(cat $(PID_FILE)) 2>/dev/null || true; rm -f $(PID_FILE); echo "✓ strategy-service stopped"; else echo "(no $(PID_FILE), nothing to stop)"; fi

clean:
	rm -rf $(PID_FILE) __pycache__
