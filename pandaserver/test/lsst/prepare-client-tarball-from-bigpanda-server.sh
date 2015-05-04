#!/bin/bash
REPOSITORY="https://github.com/PanDAWMS/panda-server.git"
BRANCH="mysql"
WRKDIR="/tmp/${USER}/bigpandaclient"


mkdir -p ${WRKDIR}
if [ ! -d ${WRKDIR} ]; then
	WRKDIR="/tmp"
fi

cd ${WRKDIR}
TARBALLNAMEBASE=bigpanda-client-mysql-0.0.1
TARBALLNAME=${TARBALLNAMEBASE}.tar.gz
TARBALLDIR=${WRKDIR}/${TARBALLNAMEBASE}
TARBALL=${WRKDIR}/${TARBALLNAME}
mkdir -p ${TARBALLDIR}
GITREPODIRNAME=panda-server
GITREPODIR=${WRKDIR}/${GITREPODIRNAME}
git clone -b ${BRANCH} ${REPOSITORY} ${GITREPODIRNAME}

cd ${GITREPODIR}/pandaserver
mkdir -p ${TARBALLDIR}/{taskbuffer,userinterface,test}

cp __init__.py ${TARBALLDIR}
cp taskbuffer/{__init__,JobSpec,FileSpec}.py ${TARBALLDIR}/taskbuffer/
cp userinterface/*.py ${TARBALLDIR}/userinterface/
cp test/lsst/*.py ${TARBALLDIR}/test/
cp ${GITREPODIR}/bigpanda-client/README.txt ${TARBALLDIR}/

cd ${WRKDIR}/
tar zcf ${TARBALL} ${TARBALLNAMEBASE}

if [ $? == 0 ]; then
	echo "BigPanDA:MySQL client is prepared in ${TARBALL}."
	echo "Please upload it to the web repository."
	echo "Please clean up ${WRKDIR} afterwards."
fi



