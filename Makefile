# ArcticDB Development Makefile
#
# Override defaults by creating a Makefile.local (gitignored).
# See Makefile.local.example for Man group configuration.

-include Makefile.local

# ── Defaults (overridable in Makefile.local or env) ──────────────────────────
RELEASE_PRESET   ?= linux-release
DEBUG_PRESET     ?= linux-debug
PROXY_CMD        ?=
CMAKE_JOBS       ?= 16
PROTOC_VERS      ?=
VENV_DIR         ?= ~/venvs
TMPDIR_OVERRIDE  ?=

# ── Environment plumbing ─────────────────────────────────────────────────────
# Prepend TMPDIR=... when TMPDIR_OVERRIDE is set
_TMPDIR_ENV := $(if $(TMPDIR_OVERRIDE),TMPDIR=$(TMPDIR_OVERRIDE))
# Set ARCTICDB_PROTOC_VERS=... when PROTOC_VERS is set
_PROTOC_ENV := $(if $(PROTOC_VERS),ARCTICDB_PROTOC_VERS=$(PROTOC_VERS))
# Combined environment prefix for commands that need both
_ENV        := $(strip $(_TMPDIR_ENV) $(_PROTOC_ENV))

# ── Phony targets ────────────────────────────────────────────────────────────
.PHONY: help setup protoc venv activate lint lint-check \
        build build-debug configure configure-debug \
        test-cpp test-cpp-debug symlink symlink-debug \
        test-py wheel bench-cpp bench-py install-editable

# ── help ─────────────────────────────────────────────────────────────────────
help: ## Show this help
	@echo "ArcticDB Development Targets"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | \
		awk -F ':.*## ' '{printf "  %-20s %s\n", $$1, $$2}'
	@echo ""
	@echo "Variables (set in Makefile.local or on command line):"
	@echo "  RELEASE_PRESET   $(RELEASE_PRESET)"
	@echo "  DEBUG_PRESET     $(DEBUG_PRESET)"
	@echo "  PROXY_CMD        $(or $(PROXY_CMD),(unset))"
	@echo "  CMAKE_JOBS       $(CMAKE_JOBS)"
	@echo "  PROTOC_VERS      $(or $(PROTOC_VERS),(unset))"
	@echo "  VENV_DIR         $(VENV_DIR)"
	@echo "  TMPDIR_OVERRIDE  $(or $(TMPDIR_OVERRIDE),(unset))"

# ── protoc ───────────────────────────────────────────────────────────────────
protoc: ## Generate protobuf stubs
	$(_ENV) $(PROXY_CMD) python setup.py protoc --build-lib python

# ── venv ─────────────────────────────────────────────────────────────────────
venv: ## Create a dev venv (NAME=<name> required, CLEAN=1 to replace existing)
ifndef NAME
	$(error NAME is required. Usage: make venv NAME=myenv)
endif
ifdef CLEAN
	rm -rf $(VENV_DIR)/$(NAME)
endif
	./build_tooling/create_venv.sh $(if $(PROXY_CMD),--proxy-cmd $(PROXY_CMD)) $(VENV_DIR)/$(NAME)

activate: ## Print venv activate path (NAME=<name> required). Use: source $$(make activate NAME=x)
ifndef NAME
	$(error NAME is required. Usage: source $$(make activate NAME=myenv))
endif
	@echo $(VENV_DIR)/$(NAME)/bin/activate

# ── setup ────────────────────────────────────────────────────────────────────
setup: ## Full setup from scratch (NAME=<name> required, CLEAN=1 to replace existing venv)
ifndef NAME
	$(error NAME is required. Usage: make setup NAME=myenv)
endif
	$(PROXY_CMD) git submodule update --init --recursive
	$(MAKE) venv NAME=$(NAME) $(if $(CLEAN),CLEAN=1)
	. $(VENV_DIR)/$(NAME)/bin/activate && \
		$(MAKE) protoc && \
		$(MAKE) build-debug && \
		$(MAKE) symlink-debug && \
		$(MAKE) activate

# ── lint ─────────────────────────────────────────────────────────────────────
lint: ## Run formatters (in-place)
	python build_tooling/format.py --in-place --type all

lint-check: ## Check formatting (no changes)
	python build_tooling/format.py --check --type all

# ── configure ────────────────────────────────────────────────────────────────
configure: ## CMake configure (release)
	$(_TMPDIR_ENV) $(PROXY_CMD) cmake -DTEST=ON --preset $(RELEASE_PRESET) cpp

configure-debug: ## CMake configure (debug)
	$(_TMPDIR_ENV) $(PROXY_CMD) cmake -DTEST=ON --preset $(DEBUG_PRESET) cpp

# ── build ────────────────────────────────────────────────────────────────────
build: configure ## Build arcticdb_ext (release)
	cmake --build cpp/out/$(RELEASE_PRESET)-build -j $(CMAKE_JOBS) --target arcticdb_ext

build-debug: configure-debug ## Build arcticdb_ext (debug)
	cmake --build cpp/out/$(DEBUG_PRESET)-build -j $(CMAKE_JOBS) --target arcticdb_ext

# ── test-cpp ─────────────────────────────────────────────────────────────────
test-cpp: ## Build and run C++ unit tests (release, FILTER= for gtest_filter)
	$(_TMPDIR_ENV) $(PROXY_CMD) cmake -DTEST=ON --preset $(RELEASE_PRESET) cpp
	cmake --build cpp/out/$(RELEASE_PRESET)-build -j $(CMAKE_JOBS) --target test_unit_arcticdb
	cpp/out/$(RELEASE_PRESET)-build/arcticdb/test_unit_arcticdb $(if $(FILTER),--gtest_filter=$(FILTER))

test-cpp-debug: ## Build and run C++ unit tests (debug, FILTER= for gtest_filter)
	$(_TMPDIR_ENV) $(PROXY_CMD) cmake -DTEST=ON --preset $(DEBUG_PRESET) cpp
	cmake --build cpp/out/$(DEBUG_PRESET)-build -j $(CMAKE_JOBS) --target test_unit_arcticdb
	cpp/out/$(DEBUG_PRESET)-build/arcticdb/test_unit_arcticdb $(if $(FILTER),--gtest_filter=$(FILTER))

# ── symlink ──────────────────────────────────────────────────────────────────
_EXT_SUFFIX := $(shell python3 -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))")

symlink: ## Symlink release arcticdb_ext into python/
	ln -sf ../cpp/out/$(RELEASE_PRESET)-build/arcticdb/arcticdb_ext$(_EXT_SUFFIX) python/arcticdb_ext$(_EXT_SUFFIX)
	@echo "Created python/arcticdb_ext$(_EXT_SUFFIX) -> release build"

symlink-debug: ## Symlink debug arcticdb_ext into python/
	ln -sf ../cpp/out/$(DEBUG_PRESET)-build/arcticdb/arcticdb_ext$(_EXT_SUFFIX) python/arcticdb_ext$(_EXT_SUFFIX)
	@echo "Created python/arcticdb_ext$(_EXT_SUFFIX) -> debug build"

# ── test-py ──────────────────────────────────────────────────────────────────
# TYPE selects the test subdirectory (unit, integration, hypothesis, stress).
# FILE overrides TYPE to run a specific file or test (strips leading python/ if present).
# ARGS passes extra arguments to pytest.
TYPE ?= unit
FILE ?=
_TEST_TARGET = $(if $(FILE),$(patsubst python/%,%,$(FILE)),tests/$(TYPE))
test-py: ## Run Python tests (TYPE=unit|integration|..., FILE= path, ARGS= extra pytest args)
	cd python && python -m pytest $(_TEST_TARGET) $(ARGS)

# ── wheel ────────────────────────────────────────────────────────────────────
wheel: ## Build a pip wheel
	$(_ENV) $(PROXY_CMD) ARCTIC_CMAKE_PRESET=$(RELEASE_PRESET) CMAKE_BUILD_PARALLEL_LEVEL=$(CMAKE_JOBS) \
		pip wheel . --no-deps -w dist/

# ── bench-cpp ────────────────────────────────────────────────────────────────
bench-cpp: ## Build and run C++ benchmarks (release, FILTER= for benchmark_filter)
	$(_TMPDIR_ENV) $(PROXY_CMD) cmake -DTEST=ON --preset $(RELEASE_PRESET) cpp
	cmake --build cpp/out/$(RELEASE_PRESET)-build -j $(CMAKE_JOBS) --target benchmarks
	cpp/out/$(RELEASE_PRESET)-build/arcticdb/benchmarks $(if $(FILTER),--benchmark_filter=$(FILTER))

# ── install-editable ─────────────────────────────────────────────────────────
install-editable: ## Install arcticdb in editable mode (no C++ rebuild)
	$(PROXY_CMD) pip install -e . --no-deps

# ── bench-py ─────────────────────────────────────────────────────────────────
bench-py: install-editable ## Run ASV Python benchmarks (BENCH= for --bench filter)
	python -m asv run --python=same -v --show-stderr $(if $(BENCH),--bench $(BENCH))
