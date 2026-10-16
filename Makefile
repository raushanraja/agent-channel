# Install, test, and wire up chan for OpenCode.
#
#   make help    # show this help
#   make         # same as `make help`
#   make install # build + install binaries + skill + MCP entry + auto-pull plugin
#   make test    # run the full Rust test gate
#   make verify  # sanity-check the installation

REPO := $(CURDIR)
PREFIX ?= $(HOME)/.local
BINDIR ?= $(PREFIX)/bin
SKILLDIR ?= $(HOME)/.config/opencode/skill
PLUGINDIR ?= $(HOME)/.config/opencode/plugins
OPENCODE_JSON ?= $(HOME)/.config/opencode/opencode.json
CHAN ?= $(REPO)/chan
BIN := $(CHAN)/target/release

.PHONY: all help build install install-bin install-skill install-mcp install-plugin verify test clean

all: help

help: ## show available make targets
	@echo "chan / orchestrator installer"
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "} {printf "  %-16s %s\n", $$1, $$2}'

build: ## build release binaries
	cd $(CHAN) && cargo build --release

install: build install-bin install-skill install-mcp install-plugin ## full install

install-bin: build ## install chan binaries
	@mkdir -p $(BINDIR)
	install -m 0755 $(BIN)/chan $(BINDIR)/chan
	install -m 0755 $(BIN)/chan-mcp $(BINDIR)/chan-mcp
	install -m 0755 $(BIN)/chan-serve $(BINDIR)/chan-serve
	@echo "binaries -> $(BINDIR)"

install-skill: ## symlink the orchestracon skill
	@mkdir -p $(SKILLDIR)
	ln -sfn $(REPO)/orchestracon $(SKILLDIR)/orchestracon
	@echo "skill -> $(SKILLDIR)/orchestracon"

install-mcp: ## add chan to opencode.json (idempotent)
	@python3 $(REPO)/scripts/register-mcp.py $(OPENCODE_JSON)

install-plugin: ## symlink the auto-pull overlay plugin
	@mkdir -p $(PLUGINDIR)
	ln -sfn $(REPO)/orchestracon/plugins/chan-auto-pull.ts $(PLUGINDIR)/chan-auto-pull.ts
	@echo "plugin -> $(PLUGINDIR)/chan-auto-pull.ts"

verify: ## sanity-check an existing installation
	@chan --version
	@test -L $(SKILLDIR)/orchestracon
	@test -L $(PLUGINDIR)/chan-auto-pull.ts
	@grep -q '"chan":' $(OPENCODE_JSON)
	@echo "install looks good"

test: ## run the full Rust gate
	cd $(CHAN) && cargo test --workspace && cargo clippy --all-targets -- -D warnings && cargo fmt --check

clean: ## remove build artifacts
	cd $(CHAN) && cargo clean