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
CLANG_TIDY       ?= clang-tidy
# Base ref that `make tidy-diff` diffs against
TIDY_BASE        ?= origin/master
# conda-forge build (see the conda-* targets)
CONDA_PRESET       ?= linux-conda-debug
CONDA_ENV_NAME     ?= arcticdb
MAMBA_ROOT_PREFIX  ?= $(HOME)/micromamba
MICROMAMBA_DIR     ?= $(HOME)/.local/micromamba
MICROMAMBA_VERSION ?= 2.1.0-0
# Overrides where the `conda-forge` channel is fetched from, e.g. an Artifactory mirror
CONDA_CHANNEL_ALIAS ?=

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
_VENV_ROOT := $(firstword $(wildcard $(if $(VIRTUAL_ENV),$(VIRTUAL_ENV))) $(VENV_DIR)/$(VENV_NAME))
_VENV_PYTHON := $(_VENV_ROOT)/bin/python
_VENV_PIP := $(VENV_DIR)/$(VENV_NAME)/bin/pip

# ── Phony targets ────────────────────────────────────────────────────────────
.PHONY: help setup protoc venv activate lint lint-check tidy tidy-diff \
        build build-debug configure configure-debug \
        test-cpp test-cpp-debug test-cpp-rapidcheck test-cpp-rapidcheck-debug symlink symlink-debug \
        test-py build-and-test-py build-and-test-py-debug \
        wheel bench-cpp bench-cpp-build bench-py install-editable \
        conda-env conda-protoc conda-configure conda-build conda-test-cpp conda-test-py conda-build-and-test-py \
        conda-shell

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
	@echo "  CLANG_TIDY       $(CLANG_TIDY)"
	@echo "  TIDY_BASE        $(TIDY_BASE)"
	@echo "  CLANG_TIDY_DIFF  $(CLANG_TIDY_DIFF)"
	@echo "  RUN_CLANG_TIDY   $(RUN_CLANG_TIDY)"
	@echo "  CONDA_PRESET     $(CONDA_PRESET)"
	@echo "  CONDA_ENV_NAME   $(CONDA_ENV_NAME)"
	@echo "  MAMBA_ROOT_PREFIX $(MAMBA_ROOT_PREFIX)"
	@echo "  CONDA_CHANNEL_ALIAS $(or $(CONDA_CHANNEL_ALIAS),(unset))"

# ── protoc ───────────────────────────────────────────────────────────────────
protoc: ## Generate protobuf stubs
	$(_ENV) $(PROXY_CMD) $(_VENV_PYTHON) setup.py protoc --build-lib python

# ── venv ─────────────────────────────────────────────────────────────────────
venv: ## Create a dev venv (VENV_NAME=<name> required, CLEAN=1 to replace existing)
ifdef CLEAN
	rm -rf $(VENV_DIR)/$(VENV_NAME)
endif
	./build_tooling/create_venv.sh $(if $(PROXY_CMD),--proxy-cmd $(PROXY_CMD)) $(VENV_DIR)/$(VENV_NAME)

activate: ## Print venv activate path (VENV_NAME=<name> required). Use: source $$(make activate VENV_NAME=x)
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

# ── clang-tidy ───────────────────────────────────────────────────────────────
# Both targets read the debug build's compile_commands.json, which must have been
# produced by clang: clang-tidy uses clang's driver, so a gcc-configured build gives
# spurious errors from libstdc++ and vcpkg headers. Set CLANG_TIDY= to a specific
# binary if clang-tidy is not on PATH (CI uses the official LLVM 19.1.7 release).
# The wrappers default to the release layout relative to that binary
# (<prefix>/share/clang/clang-tidy-diff.py, <prefix>/bin/run-clang-tidy); override
# CLANG_TIDY_DIFF= / RUN_CLANG_TIDY= for distro packages that place them elsewhere.
_TIDY_PREFIX := $(abspath $(dir $(shell command -v $(CLANG_TIDY) 2>/dev/null || echo /usr/bin/clang-tidy))..)
CLANG_TIDY_DIFF ?= $(_TIDY_PREFIX)/share/clang/clang-tidy-diff.py
RUN_CLANG_TIDY  ?= $(_TIDY_PREFIX)/bin/run-clang-tidy
_TIDY_DB := $(_DEBUG_BUILD_DIR)

tidy-diff: ## clang-tidy on changed lines only (TIDY_BASE= base ref, default origin/master)
	git diff -U0 --no-color $(TIDY_BASE)...HEAD -- '*.cpp' '*.hpp' | \
		python3 $(CLANG_TIDY_DIFF) -p1 -path $(_TIDY_DB) \
			-clang-tidy-binary $(CLANG_TIDY) -j $(CMAKE_JOBS) -quiet 2>&1 | \
		tee clang-tidy.log

tidy: ## clang-tidy over all of cpp/arcticdb
	$(RUN_CLANG_TIDY) -p $(_TIDY_DB) \
		-clang-tidy-binary $(CLANG_TIDY) -j $(CMAKE_JOBS) -quiet \
		'$(CURDIR)/cpp/arcticdb/.*' 2>&1 | tee clang-tidy.log

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

# ── test-cpp-rapidcheck ──────────────────────────────────────────────────────
test-cpp-rapidcheck: $(_RELEASE_BUILD_DIR)/.configure-stamp ## Build and run C++ rapidcheck tests (release, FILTER= for gtest_filter)
	cmake --build $(_RELEASE_BUILD_DIR) -j $(CMAKE_JOBS) --target arcticdb_rapidcheck_tests
	$(_RELEASE_BUILD_DIR)/arcticdb/arcticdb_rapidcheck_tests $(if $(FILTER),--gtest_filter=$(FILTER))

test-cpp-rapidcheck-debug: $(_DEBUG_BUILD_DIR)/.configure-stamp ## Build and run C++ rapidcheck tests (debug, FILTER= for gtest_filter)
	cmake --build $(_DEBUG_BUILD_DIR) -j $(CMAKE_JOBS) --target arcticdb_rapidcheck_tests
	$(_DEBUG_BUILD_DIR)/arcticdb/arcticdb_rapidcheck_tests $(if $(FILTER),--gtest_filter=$(FILTER))

# ── symlink ──────────────────────────────────────────────────────────────────
# EXT_SUFFIX must come from the same interpreter that test-py imports the extension with (_VENV_PYTHON),
# not whichever python3 is on PATH. Those differ whenever the venv and the system python are different
# minor versions, e.g. a py311 venv with a py310 /usr/bin/python3.
_EXT_SUFFIX_PYTHON := $(firstword $(wildcard $(_VENV_PYTHON)) python3)
_EXT_SUFFIX := $(shell $(_EXT_SUFFIX_PYTHON) -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))")

define _do_symlink
	@test -f $(1)/arcticdb/arcticdb_ext$(_EXT_SUFFIX) || { \
		echo "ERROR: $(1)/arcticdb/arcticdb_ext$(_EXT_SUFFIX) does not exist."; \
		echo "       EXT_SUFFIX '$(_EXT_SUFFIX)' came from $(_EXT_SUFFIX_PYTHON); built extensions are:"; \
		ls $(1)/arcticdb/arcticdb_ext*.so 2>/dev/null || echo "       (none — run the build first)"; \
		exit 1; }
	ln -sfn ../$(1)/arcticdb/arcticdb_ext$(_EXT_SUFFIX) python/arcticdb_ext$(_EXT_SUFFIX)
endef

symlink: ## Symlink release arcticdb_ext into python/
	$(call _do_symlink,$(_RELEASE_BUILD_DIR))
	@echo "Created python/arcticdb_ext$(_EXT_SUFFIX) -> release build"

symlink-debug: ## Symlink debug arcticdb_ext into python/
	$(call _do_symlink,$(_DEBUG_BUILD_DIR))
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
	$(_ENV) ARCTIC_CMAKE_PRESET=$(RELEASE_PRESET) CMAKE_BUILD_PARALLEL_LEVEL=$(CMAKE_JOBS) \
		$(PROXY_CMD) $(_VENV_PIP) wheel . --no-deps -w dist/

# ── bench-cpp ────────────────────────────────────────────────────────────────
bench-cpp-build: $(_RELEASE_BUILD_DIR)/.configure-stamp ## Build C++ benchmarks without running (release)
	cmake --build $(_RELEASE_BUILD_DIR) -j $(CMAKE_JOBS) --target benchmarks

bench-cpp: bench-cpp-build ## Build and run C++ benchmarks (release, FILTER= for benchmark_filter)
	$(_RELEASE_BUILD_DIR)/arcticdb/benchmarks $(if $(FILTER),--benchmark_filter=$(FILTER))

# ── install-editable ─────────────────────────────────────────────────────────
install-editable: ## Install arcticdb in editable mode
	$(PROXY_CMD) $(_VENV_PIP) install -e . --no-deps

# ── bench-py ─────────────────────────────────────────────────────────────────
bench-py: install-editable ## Run ASV Python benchmarks (BENCH= for --bench filter, QUICK=1 for single iteration)
	$(_VENV_PYTHON) -m asv run --python=same -v --show-stderr $(if $(QUICK),--quick) $(if $(BENCH),--bench $(BENCH))

# ── conda ────────────────────────────────────────────────────────────────────
# Mirrors .github/workflows/compile_and_test_with_conda.yml: a micromamba env built from
# environment-dev.yml, ARCTICDB_USING_CONDA=1 and a *-conda-* preset. All dependencies come
# from conda-forge, so vcpkg is not used.
_MICROMAMBA      := $(MICROMAMBA_DIR)/bin/micromamba
_UNAME           := $(shell uname -s)-$(shell uname -m)
_MAMBA_PLATFORM  := $(or $(if $(filter Linux-x86_64,$(_UNAME)),linux-64), \
                         $(if $(filter Linux-aarch64,$(_UNAME)),linux-aarch64), \
                         $(if $(filter Darwin-arm64,$(_UNAME)),osx-arm64),unsupported)
_MAMBA           := MAMBA_ROOT_PREFIX=$(MAMBA_ROOT_PREFIX) $(_MICROMAMBA)
_CONDA_ALIAS_ARG := $(if $(CONDA_CHANNEL_ALIAS),--channel-alias $(CONDA_CHANNEL_ALIAS))
_CONDA_RUN       := $(_MAMBA) run -n $(CONDA_ENV_NAME) env ARCTICDB_USING_CONDA=1 $(_TMPDIR_ENV)
_CONDA_PREFIX    := $(MAMBA_ROOT_PREFIX)/envs/$(CONDA_ENV_NAME)
_CONDA_BUILD_DIR := cpp/out/$(CONDA_PRESET)-build

$(_MICROMAMBA):
	mkdir -p $(MICROMAMBA_DIR)
	$(PROXY_CMD) curl -fsSL -o $(MICROMAMBA_DIR)/micromamba.tar.bz2 \
		https://github.com/mamba-org/micromamba-releases/releases/download/$(MICROMAMBA_VERSION)/micromamba-$(_MAMBA_PLATFORM).tar.bz2
	tar -xjf $(MICROMAMBA_DIR)/micromamba.tar.bz2 -C $(MICROMAMBA_DIR) bin/micromamba
	rm $(MICROMAMBA_DIR)/micromamba.tar.bz2

conda-env: $(_MICROMAMBA) ## Create or update the conda env from environment-dev.yml (CLEAN=1 to recreate)
ifdef CLEAN
	$(_MAMBA) env remove -y -n $(CONDA_ENV_NAME)
endif
	if [ -d $(_CONDA_PREFIX) ]; then \
		$(_MAMBA) install -y -n $(CONDA_ENV_NAME) -f environment-dev.yml $(_CONDA_ALIAS_ARG); \
	else \
		$(_MAMBA) create -y -n $(CONDA_ENV_NAME) -f environment-dev.yml $(_CONDA_ALIAS_ARG); \
	fi

conda-protoc: ## Generate protobuf stubs with the conda env's grpc_tools
	$(_CONDA_RUN) sh -c 'ARCTICDB_PROTOC_VERS=$$(python -c "import google.protobuf as p; print(p.__version__.split(\".\")[0])") \
		python setup.py protoc --build-lib python'

$(_CONDA_BUILD_DIR)/.configure-stamp: $(_CMAKE_INPUTS) environment-dev.yml $$(wildcard cpp/CMakeUserPresets.json)
	cd cpp && $(_CONDA_RUN) cmake -DTEST=ON --preset $(CONDA_PRESET) .
	@touch $@

conda-configure: ## CMake configure in the conda env (CONDA_PRESET=, default linux-conda-debug)
	cd cpp && $(_CONDA_RUN) cmake -DTEST=ON --preset $(CONDA_PRESET) .
	@touch $(_CONDA_BUILD_DIR)/.configure-stamp

_CONDA_EXT_SUFFIX = $(shell $(_CONDA_PREFIX)/bin/python -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))")

conda-build: $(_CONDA_BUILD_DIR)/.configure-stamp ## Build arcticdb_ext in the conda env and symlink it into python/
	$(_CONDA_RUN) cmake --build $(_CONDA_BUILD_DIR) -j $(CMAKE_JOBS) --target arcticdb_ext
	ln -sfn ../$(_CONDA_BUILD_DIR)/arcticdb/arcticdb_ext$(_CONDA_EXT_SUFFIX) python/arcticdb_ext$(_CONDA_EXT_SUFFIX)
	@echo "Created python/arcticdb_ext$(_CONDA_EXT_SUFFIX) -> $(CONDA_PRESET) build"

conda-test-cpp: $(_CONDA_BUILD_DIR)/.configure-stamp ## Build and run C++ unit tests in the conda env (FILTER= for gtest_filter)
	$(_CONDA_RUN) cmake --build $(_CONDA_BUILD_DIR) -j $(CMAKE_JOBS) --target test_unit_arcticdb
	$(_CONDA_RUN) $(_CONDA_BUILD_DIR)/arcticdb/test_unit_arcticdb $(if $(FILTER),--gtest_filter=$(FILTER))

conda-test-py: ## Run Python tests in the conda env (TYPE=, FILE=, ARGS= as for test-py)
	cd python && $(_CONDA_RUN) python -m pytest $(_TEST_TARGET) $(ARGS)

conda-build-and-test-py: conda-build ## conda build + symlink + run Python tests
	$(MAKE) conda-test-py

conda-shell: ## Open a shell with the conda env activated and ARCTICDB_USING_CONDA=1
	$(_CONDA_RUN) bash
