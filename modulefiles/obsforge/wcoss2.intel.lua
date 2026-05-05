help([[
Load environment for running the ObsForge application with Intel compilers and MPI.
]])

local pkgName    = myModuleName()
local pkgVersion = myModuleVersion()
local pkgNameVer = myModuleFullName()

prepend_path("MODULEPATH", "/apps/dev/lmodules/core")

local PrgEnv_intel_ver = os.getenv("PrgEnv_intel_ver") or "8.5.0"
local cmake_ver = os.getenv("cmake_ver") or "3.27.9"
local craype_ver = os.getenv("craype_ver") or "2.7.17"
local cray_pals_ver = os.getenv("cray_pals_ver") or "1.3.2"
local git_ver = os.getenv("git_ver") or "2.29.0"
local intel_ver = os.getenv("intel_ver") or "19.1.3.304"
local cray_mpich_ver = os.getenv("cray_mpich_ver") or "8.1.19"
local hdf5D_ver = os.getenv("hdf5D_ver") or os.getenv("hdf5_ver") or "1.14.0"
local pnetcdfD_ver = os.getenv("pnetcdfD_ver") or os.getenv("pnetcdf_ver") or "1.12.2"
local netcdfD_ver = os.getenv("netcdfD_ver") or os.getenv("netcdf_ver") or "4.9.2"
local udunits_ver = os.getenv("udunits_ver") or "2.2.28"
local eigen_ver = os.getenv("eigen_ver") or "3.4.0"
local boost_ver = os.getenv("boost_ver") or "1.79.0"
local gsl_lit_ver = os.getenv("gsl_lit_ver") or "v0.40.0"
local sp_ver = os.getenv("sp_ver") or "2.4.0"
local python_ver = os.getenv("python_ver") or "3.8.6"
local ecbuild_ver = os.getenv("ecbuild_ver") or "3.7.0"
local qhull_ver = os.getenv("qhull_ver") or "2020.2"
local eckit_ver = os.getenv("eckit_ver") or "1.28.3"
local fckit_ver = os.getenv("fckit_ver") or "0.13.2"
local atlas_ver = os.getenv("atlas_ver") or "0.44.1"
local nco_ver = os.getenv("nco_ver") or "5.2.4"
local gsl_ver = os.getenv("gsl_ver") or "2.7"
local prod_util_ver = os.getenv("prod_util_ver") or "2.0.14"
local bufr_ver = os.getenv("bufr_ver") or "12.1.0"
local fmsD_ver = os.getenv("fmsD_ver") or "2024.01"

load("PrgEnv-intel/" .. PrgEnv_intel_ver)
load("cmake/" .. cmake_ver)
load("craype/" .. craype_ver)
load("cray-pals/" .. cray_pals_ver)
load("git/" .. git_ver)
load("intel/" .. intel_ver)
load("cray-mpich/" .. cray_mpich_ver)
load("hdf5-D/" .. hdf5D_ver)
load("pnetcdf-D/" .. pnetcdfD_ver)
load("netcdf-D/" .. netcdfD_ver)
load("udunits/" .. udunits_ver)
load("eigen/" .. eigen_ver)
load("boost/" .. boost_ver)
load("gsl-lite/" .. gsl_lit_ver)
load("sp/" .. sp_ver)
load("python/" .. python_ver)
load("ecbuild/" .. ecbuild_ver)
load("qhull/" .. qhull_ver)
load("eckit/" .. eckit_ver)
load("fckit/" .. fckit_ver)
load("atlas/" .. atlas_ver)
load("nco/" .. nco_ver)
load("gsl/" .. gsl_ver)
load("prod_util/" .. prod_util_ver)
load("bufr/" .. bufr_ver)
load("fms-D/" .. fmsD_ver)

local mpiexec = '/pe/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/bin/mpirun'
local mpinproc = '-n'
setenv('MPIEXEC_EXEC', mpiexec)
setenv('MPIEXEC_NPROC', mpinproc)

whatis("Name: ".. pkgName)
whatis("Version: ".. pkgVersion)
whatis("Category: GDASApp")
whatis("Description: Load all libraries needed for GDASApp")
