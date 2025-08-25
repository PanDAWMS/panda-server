# Core package for PanDA server

## PanDA documentation 

https://panda-wms.readthedocs.io/en/latest/

## PanDA server installation instructions

https://panda-wms.readthedocs.io/en/latest/installation/server.html

## PanDA server cluster details for ATLAS

https://github.com/PanDAWMS/panda-server/wiki/ATLAS-production-PanDA-servers-%5BSeptember-2023%5D

## Installation and Distribution
### Installation via PyPI
``` conslole
$ pip install panda-server
```

### Installation from GitHub repository
``` console
$ pip install git+https://github.com/PanDAWMS/panda-server.git#subdirectory=server
```

### Installation from local Git clone
``` console
$ git clone
$ cd panda-server/server
$ pip install .
```

### Making source distribution to be published on PyPI
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