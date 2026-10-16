# Install, test, and wire up chan for OpenCode.
#
#   make            # same as `make install`
#   make build      # build release binaries
#   make install    # build + install binaries + skill + MCP entry + auto-pull plugin
#   make test       # run the full Rust test gate
#   make verify     # sanity-check the installation

REPO := $(CURDIR)
PREFIX ?= $(HOME)/.local
BINDIR ?= $(PREFIX)/bin
SKILLDIR ?= $(HOME)/.config/opencode/skill
PLUGINDIR ?= $(HOME)/.config/opencode/plugins
OPENCODE_JSON ?= $(HOME)/.config/opencode/opencode.json
CHAN ?= $(REPO)/chan
BIN := $(CHAN)/target/release

.PHONY: all build install install-bin install-skill install-mcp install-plugin verify test clean

all: install

build:
	cd $(CHAN) && cargo build --release

install: build install-bin install-skill install-mcp install-plugin
	@echo
	@echo "chan installed. Restart OpenCode / opencode serve to pick up the MCP server, skill, and plugin."

install-bin: build
	@mkdir -p $(BINDIR)
	install -m 0755 $(BIN)/chan $(BINDIR)/chan
	install -m 0755 $(BIN)/chan-mcp $(BINDIR)/chan-mcp
	install -m 0755 $(BIN)/chan-serve $(BINDIR)/chan-serve
	@echo "binaries -> $(BINDIR)"

install-skill:
	@mkdir -p $(SKILLDIR)
	ln -sfn $(REPO)/orchestracon $(SKILLDIR)/orchestracon
	@echo "skill -> $(SKILLDIR)/orchestracon"

install-mcp:
	@python3 $(REPO)/scripts/register-mcp.py $(OPENCODE_JSON)

install-plugin:
	@mkdir -p $(PLUGINDIR)
	ln -sfn $(REPO)/orchestracon/plugins/chan-auto-pull.ts $(PLUGINDIR)/chan-auto-pull.ts
	@echo "plugin -> $(PLUGINDIR)/chan-auto-pull.ts"

verify:
	@chan --version
	@test -L $(SKILLDIR)/orchestracon
	@test -L $(PLUGINDIR)/chan-auto-pull.ts
	@grep -q '"chan":' $(OPENCODE_JSON)
	@echo "install looks good"

test:
	cd $(CHAN) && cargo test --workspace && cargo clippy --all-targets -- -D warnings && cargo fmt --check

clean:
	cd $(CHAN) && cargo clean