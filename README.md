# Monorepo for PanDA Services

## Overall PanDA documentation 

https://panda-wms.readthedocs.io/en/latest/

## Installation and distribution
### Installation via PyPI
``` conslole
$ pip install panda-server
```

### Installation from GitHub repository
``` console
$ pip install git+https://github.com/PanDAWMS/panda-server.git
```

### Installation from local Git clone
``` console
$ git clone
$ cd panda-server
$ pip install .
```

### Making source distribution to be published on PyPI
``` console
$ cd panda-server
$ python -m build -s
```

## Making Unified Docker image
``` console
$ git clone
$ cd panda-server
$ docker build .
```