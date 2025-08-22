# Core package for PanDA server

## PanDA documentation 

https://panda-wms.readthedocs.io/en/latest/

## PanDA server installation instructions

https://panda-wms.readthedocs.io/en/latest/installation/server.html

## Release notes

See ChangeLog.txt

## PanDA server cluster details for ATLAS

https://github.com/PanDAWMS/panda-server/wiki/ATLAS-production-PanDA-servers-%5BSeptember-2023%5D

## Installation and Distribution
### PyPI
``` conslole
$ pip install panda-server
```

### Local installation from git clone
``` console
$ git clone
$ cd panda-server/server
$ pip install .
```

### Making source distribution
``` console
$ cd panda-server/server
$ mv ../pandaserver .
$ mv ../PandaPkgInfo.py .
$ sed -i 's|"../|"|g' pyproject.toml
$ python -m build -s
```

### Making Docker image
``` console
$ cd panda-server
$ docker build -f server/Dockerfile
```