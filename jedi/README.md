# JEDI Package

## JEDI cluster details for ATLAS

https://github.com/PanDAWMS/panda-jedi/wiki/ATLAS-production-JEDI-servers-%5BSeptember-2023%5D

## Installation and distribution
### Installation via PyPI
``` conslole
$ pip install panda-jedi
```

### Installation from GitHub repository
``` console
$ pip install git+https://github.com/PanDAWMS/panda-server.git#subdirectory=jedi
```

### Installation from local Git clone
``` console
$ git clone
$ cd panda-server/jedi
$ pip install .
```

### Making source distribution to be published on PyPI
``` console
$ cd panda-server/jedi
$ mv ../pandajedi .
$ mv ../PandaPkgInfo.py .
$ sed -i 's|"../|"|g' pyproject.toml
$ python -m build -s
```
