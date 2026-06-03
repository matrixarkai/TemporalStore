## Build and run docker image
```bash
sudo make build
sudo make run
```

## Switch gcc version
```bash
# export module functions
source $MODULESHOME/init/bash
# switch to gcc630
# it adds gcc630-related paths to PATH, LD_LIBRARY_PATH, etc.
module switch gcc/6.3.0
# or switch to gcc830
# it adds gcc830-related paths to PATH, LD_LIBRARY_PATH, etc.
module switch gcc/8.3.0
```

## Other module functions
Refer to https://modules.readthedocs.io/en/latest/module.html for more details.

```bash
# list available modules
module avail
# list loaded modules
module list
# unload a module (e.g., gcc)
# it removes module-related paths from PATH, LD_LIBRARY_PATH, etc.
module unload gcc
# load a module with its default version
# it adds module-related paths to PATH, LD_LIBRARY_PATH, etc.
module load gcc
# load a module with specified version
# it adds module-related paths to PATH, LD_LIBRARY_PATH, etc.
module load gcc/6.3.0
```