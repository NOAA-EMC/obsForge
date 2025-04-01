# obsForge
Forging next generation of observation processing leveraging JEDI

# Clone and Build
```
git clone --recursive --jobs 2 https://github.com/NOAA-EMC/obsForge.git
cd obsForge
./build.sh
```

# Tests
Load the modules if you have not yet,
```
module use modulefiles
module load obsforge/{hpc}.{compiler}
```

Testing the bufr to ioda converters:
```
cd build/obsForge
ctest
```

Testing the non-bufr to ioda converters:
```
cd build
ctest -R test_obsforge_util
```



# Workflow usage
```console
export HOMEobsforge=<path to obsForge>
export PYTHONPATH="${PYTHONPATH}:${HOMEobsforge}/sorc/wxflow/src:${HOMEobsforge}/ush/python"
setup_xml.py --config config.yaml  --template obsforge_rocoto_template.xml.j2 --output obsforge.xml
```
