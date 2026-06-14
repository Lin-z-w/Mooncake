# Repository Guidelines

## Project Structure & Module Organization

Mooncake is a mixed C++, Python, Go, and Rust repository organized as CMake modules. Core transfer logic lives in `mooncake-transfer-engine/`; distributed KV cache services and clients are in `mooncake-store/`; shared utilities are in `mooncake-common/`; Python bindings and package integrations are in `mooncake-integration/` and `mooncake-wheel/`. Other components include `mooncake-p2p-store/`, `mooncake-ep/`, `mooncake-pg/`, and `mooncake-rl/`. Tests are usually under each module's `tests/` directory. Documentation is in `docs/`, scripts in `scripts/`, benchmarks in `benchmarks/`, and README assets in `image/`.

## Build, Test, and Development Commands

- `sudo bash dependencies.sh -y`: install dependencies, initialize submodules, and install third-party libraries.
- `mkdir -p build && cd build && cmake ..`: configure a default local build.
- `cmake --build build -j$(nproc)`: build all configured targets.
- `cmake --install build`: install built binaries and libraries.
- `ctest --test-dir build --output-on-failure`: run C++/CMake tests; add `-R <pattern>` for a focused test.
- `./scripts/run_tests.sh`: run the Python package integration test flow after build/install.
- `pre-commit run --all-files`: run formatting, linting, spelling, and CMake checks before submitting changes.

Use CMake flags to match your environment, for example `-DUSE_CUDA=OFF`, `-DUSE_HTTP=ON`, `-DSTORE_USE_ETCD=ON`, or `-DBUILD_UNIT_TESTS=ON`.

## Coding Style & Naming Conventions

Follow Google C++ and Google Python style. `.clang-format` uses Google style with 4-space indentation, no tabs, an 80-column limit, and unsorted includes. Python is checked with `ruff` and `ruff-format`; CMake is checked with `cmake-format`; hygiene hooks include trailing whitespace, end-of-file fixes, YAML checks, and codespell. Install hooks with `pip install -r requirements-dev.txt && pre-commit install`.

Prefer descriptive names consistent with nearby code. C++ tests generally use `*_test.cpp`; Python tests use `test_*.py`.

## Testing Guidelines

Add or update tests with the module you change: C++ tests under `mooncake-store/tests/` or `mooncake-transfer-engine/tests/`, Python package tests under `mooncake-wheel/tests/`, and Go tests next to their packages. Run focused tests first, then the relevant broader suite. Some integration tests require local services such as `mooncake_master`, metadata server settings like `MC_METADATA_SERVER=http://127.0.0.1:8080/metadata`, or optional hardware/runtime flags.

## Commit & Pull Request Guidelines

Recent history and `CONTRIBUTING.md` use scoped prefixes such as `[Store]`, `[TransferEngine]`, `[P2PStore]`, `[Integration]`, `[CI/Build]`, `[Doc]`, `[Bugfix]`, and `[Misc]`. Commit subjects are short, imperative summaries; many use `fix:`, `feat:`, `docs:`, or `chore:` after the scope.

Do not run force-push commands, including `git push --force` or `git push --force-with-lease`, unless the user explicitly approves that specific action first.

For PRs, include a clear description, affected component, test results, and linked issues. Major architectural changes over roughly 500 LOC excluding tests should start with a GitHub RFC issue. Update `docs/` when user-facing behavior changes.
