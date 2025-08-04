# obsForge
Forging next generation of observation processing leveraging JEDI

## Clone and Build
```bash
git clone --recursive --jobs 2 https://github.com/NOAA-EMC/obsForge.git
cd obsForge
./build.sh
```

## Testing

This section provides instructions for running all tests in the main obsForge repository (excluding submodule tests). The testing includes both C++ tests managed by CTest and Python tests using pytest.

### Prerequisites

1. **Build the project** (if not already done):
   ```bash
   ./build.sh
   ```

2. **Load modules** (on supported HPC systems):
   ```bash
   module use modulefiles
   module load obsforge/{hpc}.{compiler}
   ```

3. **Set up Python environment** for pytest:
   ```bash
   python3 -m venv obsforge_test_env
   source obsforge_test_env/bin/activate
   pip install -r requirements.txt
   ```

### Running C++ Tests (CTest)

All CTest commands should be run from the build directory:

```bash
cd build
```

**Run all tests:**
```bash
ctest
```

**Run tests with verbose output on failure:**
```bash
ctest --output-on-failure
```

**Run tests in parallel (faster execution):**
```bash
ctest -j$(nproc)
```

**Run specific test categories:**

- BUFR to IODA converter tests:
  ```bash
  cd obsForge
  ctest
  ```

- Non-BUFR to IODA converter tests (utility tests):
  ```bash
  ctest -R test_obsforge_util
  ```

- BUFR-to-IODA specific tests:
  ```bash
  ctest -R test_b2i
  ```

### Running Python Tests (pytest)

**Activate Python environment** (if not already activated):
```bash
source obsforge_test_env/bin/activate
```

**Run all pytest tests:**
```bash
# Test the pyobsforge module
pytest ush/python/pyobsforge/tests/ --disable-warnings -v

# Test the scripts
pytest scripts/tests/ --disable-warnings -v
```

**Run tests with style checking:**
```bash
# Install flake8 if not already installed
pip install flake8

# Check code style
flake8 ush/python/pyobsforge
flake8 ush/*.py
flake8 scripts/*.py

# Run pytest tests
pytest ush/python/pyobsforge/tests/ --disable-warnings -v
pytest scripts/tests/ --disable-warnings -v
```

### Running All Tests (Combined)

To run both CTest and pytest tests in sequence:

```bash
# From the repository root
cd build
ctest --output-on-failure -j$(nproc)
cd ..
source obsforge_test_env/bin/activate
pytest ush/python/pyobsforge/tests/ scripts/tests/ --disable-warnings -v
```

### Notes

- **Submodule tests are excluded** from these instructions - they have their own testing procedures
- Use `ctest --output-on-failure` to see detailed error messages when tests fail
- Parallel execution with `-j$(nproc)` can significantly speed up test runs
- The Python virtual environment setup is recommended to avoid dependency conflicts
- Some tests may require specific data files or external dependencies that are downloaded automatically during the build process

## Workflow Usage
```console
source ush/of_setup.sh
setup_xml.py --config config.yaml  --template obsforge_rocoto_template.xml.j2 --output obsforge.xml
```

#### Note:
To load `rocoto` on WCOSS2:
```
module use /apps/ops/test/nco/modulefiles/core
module load rocoto
```
