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
VENV_NAME		 ?= dev-venv
TMPDIR_OVERRIDE  ?=

# ── Build directory paths ────────────────────────────────────────────────────
_RELEASE_BUILD_DIR := cpp/out/$(RELEASE_PRESET)-build
_DEBUG_BUILD_DIR   := cpp/out/$(DEBUG_PRESET)-build

# ── Environment plumbing ─────────────────────────────────────────────────────
# Prepend TMPDIR=... when TMPDIR_OVERRIDE is set
_TMPDIR_ENV := $(if $(TMPDIR_OVERRIDE),TMPDIR=$(TMPDIR_OVERRIDE))
# Set ARCTICDB_PROTOC_VERS=... when PROTOC_VERS is set
_PROTOC_ENV := $(if $(PROTOC_VERS),ARCTICDB_PROTOC_VERS=$(PROTOC_VERS))
# Combined environment prefix for commands that need both
_ENV        := $(strip $(_TMPDIR_ENV) $(_PROTOC_ENV))
_VENV_PYTHON := $(VENV_DIR)/$(VENV_NAME)/bin/python
_VENV_PIP := $(VENV_DIR)/$(VENV_NAME)/bin/pip

# ── Phony targets ────────────────────────────────────────────────────────────
.PHONY: help setup protoc venv activate lint lint-check \
        build build-debug configure configure-debug \
        test-cpp test-cpp-debug symlink symlink-debug \
        test-py build-and-test-py build-and-test-py-debug \
        wheel bench-cpp bench-py install-editable

# ── help ─────────────────────────────────────────────────────────────────────
help: ## Show this help
	@echo "ArcticDB Development Targets"
	@echo ""
	@grep -hE '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | \
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
	$(_ENV) $(PROXY_CMD) $(_VENV_PYTHON) setup.py protoc --build-lib python

# ── venv ─────────────────────────────────────────────────────────────────────
venv: ## Create a dev venv (NAME=<name> required, CLEAN=1 to replace existing)
ifdef CLEAN
	rm -rf $(VENV_DIR)/$(VENV_NAME)
endif
	./build_tooling/create_venv.sh $(if $(PROXY_CMD),--proxy-cmd $(PROXY_CMD)) $(VENV_DIR)/$(VENV_NAME)

activate: ## Print venv activate path (NAME=<name> required). Use: source $$(make activate NAME=x)
	@echo $(VENV_DIR)/$(VENV_NAME)/bin/activate

# ── setup ────────────────────────────────────────────────────────────────────
setup: ## Full setup from scratch (CLEAN=1 to replace existing venv)
	$(PROXY_CMD) git submodule update --init --recursive
	$(MAKE) venv $(if $(CLEAN),CLEAN=1)
	. $(VENV_DIR)/$(VENV_NAME)/bin/activate && \
		$(MAKE) protoc && \
		$(MAKE) build-debug && \
		$(MAKE) activate

# ── lint ─────────────────────────────────────────────────────────────────────
install-linters:
	$(PROXY_CMD) $(_VENV_PYTHON) build_tooling/format.py --install-tools

lint: ## Run formatters (in-place)
	$(_VENV_PYTHON) build_tooling/format.py --in-place --type all

lint-check: ## Check formatting (no changes)
	$(_VENV_PYTHON) build_tooling/format.py --check --type all

# ── configure ────────────────────────────────────────────────────────────────
# Files whose changes should trigger a cmake reconfigure.
# Makefile.local is optional; $(wildcard) at parse time is fine since the file
# is either always present or always absent for a given developer's machine.
_CMAKE_INPUTS := cpp/CMakeLists.txt cpp/CMakePresets.json cpp/vcpkg.json \
                 $(wildcard Makefile.local)

# .configure-stamp is our own sentinel file — cmake never touches it, so its
# mtime is fully under our control and won't interfere with cmake's internal
# check-build-system mechanism (which uses CMakeFiles/cmake.check_cache).
# cmake --build handles its own incremental reconfigure for files it tracks.
#
# CMakeUserPresets.json is evaluated via .SECONDEXPANSION ($$(wildcard ...)) so
# that it is detected at rule-evaluation time rather than parse time — correctly
# handling the case where a developer creates the file for the first time after
# an initial build.
.SECONDEXPANSION:
$(_RELEASE_BUILD_DIR)/.configure-stamp: $(_CMAKE_INPUTS) $$(wildcard cpp/CMakeUserPresets.json)
	$(_TMPDIR_ENV) $(PROXY_CMD) cmake -DTEST=ON --preset $(RELEASE_PRESET) cpp
	@touch $@

$(_DEBUG_BUILD_DIR)/.configure-stamp: $(_CMAKE_INPUTS) $$(wildcard cpp/CMakeUserPresets.json)
	$(_TMPDIR_ENV) $(PROXY_CMD) cmake -DTEST=ON --preset $(DEBUG_PRESET) cpp
	@touch $@

configure: ## CMake configure (release) — always reconfigures
	$(_TMPDIR_ENV) $(PROXY_CMD) cmake -DTEST=ON --preset $(RELEASE_PRESET) cpp
	@touch $(_RELEASE_BUILD_DIR)/.configure-stamp

configure-debug: ## CMake configure (debug) — always reconfigures
	$(_TMPDIR_ENV) $(PROXY_CMD) cmake -DTEST=ON --preset $(DEBUG_PRESET) cpp
	@touch $(_DEBUG_BUILD_DIR)/.configure-stamp

# ── build ────────────────────────────────────────────────────────────────────
build: $(_RELEASE_BUILD_DIR)/.configure-stamp ## Build arcticdb_ext (release) and symlink
	cmake --build $(_RELEASE_BUILD_DIR) -j $(CMAKE_JOBS) --target arcticdb_ext
	$(MAKE) symlink

build-debug: $(_DEBUG_BUILD_DIR)/.configure-stamp ## Build arcticdb_ext (debug) and symlink
	cmake --build $(_DEBUG_BUILD_DIR) -j $(CMAKE_JOBS) --target arcticdb_ext
	$(MAKE) symlink-debug

# ── test-cpp ─────────────────────────────────────────────────────────────────
test-cpp: $(_RELEASE_BUILD_DIR)/.configure-stamp ## Build and run C++ unit tests (release, FILTER= for gtest_filter)
	cmake --build $(_RELEASE_BUILD_DIR) -j $(CMAKE_JOBS) --target test_unit_arcticdb
	$(_RELEASE_BUILD_DIR)/arcticdb/test_unit_arcticdb $(if $(FILTER),--gtest_filter=$(FILTER))

test-cpp-debug: $(_DEBUG_BUILD_DIR)/.configure-stamp ## Build and run C++ unit tests (debug, FILTER= for gtest_filter)
	cmake --build $(_DEBUG_BUILD_DIR) -j $(CMAKE_JOBS) --target test_unit_arcticdb
	$(_DEBUG_BUILD_DIR)/arcticdb/test_unit_arcticdb $(if $(FILTER),--gtest_filter=$(FILTER))

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
	cd python && $(_VENV_PYTHON) -m pytest $(_TEST_TARGET) $(ARGS)

build-and-test-py: build ## Release build + symlink + run Python tests
	$(MAKE) test-py

build-and-test-py-debug: build-debug ## Debug build + symlink + run Python tests
	$(MAKE) test-py

# ── wheel ────────────────────────────────────────────────────────────────────
wheel: ## Build a pip wheel
	$(_ENV) $(PROXY_CMD) ARCTIC_CMAKE_PRESET=$(RELEASE_PRESET) CMAKE_BUILD_PARALLEL_LEVEL=$(CMAKE_JOBS) \
		$(_VENV_PIP) wheel . --no-deps -w dist/

# ── bench-cpp ────────────────────────────────────────────────────────────────
bench-cpp: $(_RELEASE_BUILD_DIR)/.configure-stamp ## Build and run C++ benchmarks (release, FILTER= for benchmark_filter)
	cmake --build $(_RELEASE_BUILD_DIR) -j $(CMAKE_JOBS) --target benchmarks
	$(_RELEASE_BUILD_DIR)/arcticdb/benchmarks $(if $(FILTER),--benchmark_filter=$(FILTER))

# ── install-editable ─────────────────────────────────────────────────────────
install-editable: ## Install arcticdb in editable mode
	$(PROXY_CMD) $(_VENV_PIP) install -e . --no-deps

# ── bench-py ─────────────────────────────────────────────────────────────────
bench-py: install-editable ## Run ASV Python benchmarks (BENCH= for --bench filter)
	$(_VENV_PYTHON) -m asv run --python=same -v --show-stderr $(if $(BENCH),--bench $(BENCH))
