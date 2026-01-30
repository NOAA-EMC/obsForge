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
   source ush/of_setup.sh
   ```

3. **Set up Python environment** for pytest:
   ```bash
   python3 -m venv obsforge_test_env
   source obsforge_test_env/bin/activate
   pip install -r requirements.txt
   ```
   **Note:** This virtual environment setup is essential when using JCSDA docker containers and recommended for local development to avoid dependency conflicts. It may not be necessary if you've already loaded appropriate modules on HPC systems, but it's still recommended for isolation.

### Running C++ Tests (CTest)

All CTest commands should be run from the build directory. **Note:** These commands run only obsForge-specific tests, excluding submodule tests (OOPS, IODA, etc.).

```bash
cd build
```

**Important for HPC Users:** 
- Some tests use MPI and cannot run on login nodes
- Start an interactive compute node session before running tests:
  ```bash
  # Example for SLURM systems
  salloc -N 1 --ntasks-per-node=4 --time=1:00:00
  # or
  srun --pty -N 1 --ntasks-per-node=4 --time=1:00:00 /bin/bash
  ```

**Run obsForge-specific tests:**
```bash
# Run obsforge-utils tests only
cd obsforge-utils
ctest --output-on-failure
```

**Run tests in parallel (faster execution):**
```bash
cd obsforge-utils
ctest -j$(nproc) --output-on-failure
```

**Run specific test categories:**

- BUFR to IODA converter tests:
  ```bash
  cd obsforge-utils
  ctest -R test_b2i --output-on-failure
  ```

- Non-BUFR to IODA converter tests (utility tests):
  ```bash
  cd obsforge-utils
  ctest -R test_obsforge_util --output-on-failure
  ```

**Note:** Some tests may require specific data files that are only available on certain HPC systems (WCOSS2, Hera, etc.). If tests fail due to missing data, they may need to be run on the appropriate HPC environment.

### Running Python Tests (pytest)

**Activate Python environment** (if not already activated):
```bash
source obsforge_test_env/bin/activate
```

**Run pytest tests:**
```bash
# Test the pyobsforge module
pytest ush/python/pyobsforge/tests/ --disable-warnings -v

# Test the scripts (Note: may require HPC-specific data)
pytest scripts/tests/ --disable-warnings -v
```

**Important:** The `scripts/tests/` tests may require specific data files that are only staged on certain HPC systems. If these tests fail due to missing data, they should be run on the appropriate HPC environment where the data is available.

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
cd build/obsforge-utils
ctest --output-on-failure -j$(nproc)
cd ../..
source obsforge_test_env/bin/activate
pytest ush/python/pyobsforge/tests/ --disable-warnings -v
# Only run scripts tests if on HPC with required data
# pytest scripts/tests/ --disable-warnings -v
```

### Notes

- **Submodule tests are excluded** from these instructions - they have their own testing procedures
- **HPC considerations:** 
  - Some tests use MPI and cannot run on login nodes
  - Use interactive compute node sessions (`salloc` or `srun`) when on HPC systems
  - Some tests require data files only available on specific HPC systems (WCOSS2, Hera, etc.)
- Use `ctest --output-on-failure` to see detailed error messages when tests fail
- Parallel execution with `-j$(nproc)` can significantly speed up test runs
- The Python virtual environment setup is essential for JCSDA docker containers and recommended for local development to avoid dependency conflicts
- Focus on `obsforge-utils` directory for C++ tests to avoid running all submodule tests

## Workflow Usage
```console
cd ush
source of_setup.sh
setup_xml.py --config ../parm/config.yaml  --template ../parm/obsforge_rocoto_template.xml.j2 --output obsforge.xml
rocotorun -d obsforge.db -w obsforge.xml
```

#### Note:
To load `rocoto` on WCOSS2:
```
module use /apps/ops/test/nco/modulefiles/core
module load rocoto
```
