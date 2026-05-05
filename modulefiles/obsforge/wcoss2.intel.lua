help([[
Load environment for running the GDAS application with Intel compilers and MPI.
]])

local pkgName    = myModuleName()
local pkgVersion = myModuleVersion() or "1.0"
local pkgNameVer = myModuleFullName()

prepend_path("MODULEPATH", "/apps/ops/test/spack-stack-nco-1.9/modulefiles/Core")

local oneapi_ver    = os.getenv("oneapi_ver")    or "2024.2.1"
local cray_mpich_ver = os.getenv("cray_mpich_ver") or "8.1.29"
local python_ver    = os.getenv("python_ver")    or "3.11.7"
local cmake_ver     = os.getenv("cmake_ver")     or "3.27.9"
local craype_ver    = os.getenv("craype_ver")    or "2.7.17"
local cray_pals_ver = os.getenv("cray_pals_ver") or "1.3.2"
local hdf5_ver      = os.getenv("hdf5_ver")      or "1.14.3"
local pnetcdf_ver   = os.getenv("pnetcdf_ver")   or "1.12.3"
local netcdf_ver    = os.getenv("netcdf_ver")    or "4.9.2"
local boost_ver     = os.getenv("boost_ver")     or "1.84.0"
local eigen_ver     = os.getenv("eigen_ver")     or "3.4.0"
local eckit_ver     = os.getenv("eckit_ver")     or "1.28.3"
local fckit_ver     = os.getenv("fckit_ver")     or "0.13.2"
local esmf_ver      = os.getenv("esmf_ver")      or "8.8.0"
local udunits_ver   = os.getenv("udunits_ver")   or "2.2.28"
local libpng_ver    = os.getenv("libpng_ver")    or "1.6.37"
local bufr_ver      = os.getenv("bufr_ver")      or "12.1.0"
local prod_util_ver = os.getenv("prod_util_ver") or "2.0.14"

load("stack-oneapi/" .. oneapi_ver)
load("stack-cray-mpich/" .. cray_mpich_ver)
load("stack-python/" .. python_ver)
load("cmake/" .. cmake_ver)
load("craype/" .. craype_ver)
load("cray-pals/" .. cray_pals_ver)

load("git/2.47.0")
load("git-lfs/3.5.1")

load("zstd/1.5.6")
load("pigz/2.8")
load("tar/1.34")
load("gettext/0.22.5")
load("curl/8.10.1")
load("hdf5/" .. hdf5_ver)
load("parallel-netcdf/" .. pnetcdf_ver)
load("netcdf-c/" .. netcdf_ver)
load("nccmp/1.9.0.1")
load("netcdf-fortran/4.6.1")
load("nco/5.2.4")
load("parallelio/2.6.2")
load("wget/1.21.1")
load("boost/" .. boost_ver)
load("ecbuild/3.7.2")
load("openjpeg/2.5.0")
load("eigen/" .. eigen_ver)
load("openblas/0.3.24")
load("eckit/" .. eckit_ver)
load("fckit/" .. fckit_ver)
load("python-venv/1.0")
load("py-pyyaml/6.0.2")
load("intel-oneapi-runtime/" .. oneapi_ver)
load("glibc/2.31")
load("esmf/" .. esmf_ver)
load("atlas/0.40.0")
load("sp/2.5.0")
load("ip/5.1.0")
load("gsl-lite/0.37.0")
load("libjpeg/2.1.0")
load("hdf/4.2.15")
load("jedi-cmake/1.4.0")
load("libpng/" .. libpng_ver)
load("udunits/" .. udunits_ver)
--load("ncview/2.1.9")
load("netcdf-cxx4/4.3.1")

load("prod_util/" .. prod_util_ver)
load("py-numpy/1.26.4")

load("py-markupsafe/2.1.3")
load("py-jinja2/3.1.4")
load("py-cftime/1.0.3.4")
load("py-certifi/2023.7.22")
load("py-netcdf4/1.7.1.post2")
load("py-pybind11/2.13.5")
load("py-setuptools/63.4.3")
load("py-pycodestyle/2.11.0")
load("py-pyyaml/6.0.2")
load("py-scipy/1.14.1")

load("py-setuptools/63.4.3")
load("py-tzdata/2023.3")
load("py-pytz/2023.3")
load("py-six/1.16.0")
load("py-python-dateutil/2.8.2")
load("py-pandas/2.2.3")
load("py-packaging/24.1")
load("py-xarray/2024.7.0")
load("py-f90nml/1.4.3")
load("py-pip/23.1.2")
load("py-click/8.1.7")
load("py-wheel/0.41.2")

load("bufr/12.1.0")

append_path("MODULEPATH", "/apps/ops/test/nco/modulefiles/core")
load("rocoto/1.3.5")

setenv("CC","cc")
setenv("CXX","CC")
setenv("FC","ifort")

local mpiexec = '/opt/cray/pals/1.3.2/bin/mpirun'
local mpinproc = '-n'
setenv('MPIEXEC_EXEC', mpiexec)
setenv('MPIEXEC_NPROC', mpinproc)

whatis("Name: ".. pkgName)
whatis("Version: ".. pkgVersion)
whatis("Category: GDASApp")
whatis("Description: Load all libraries needed for GDASApp")
