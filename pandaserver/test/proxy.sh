#!/bin/bash -l

echo '************** start'
date
source /afs/cern.ch/project/gd/LCG-share/current/external/etc/profile.d/grid-env.sh
echo '************** check proxy'
voms-proxy-info -all
echo '************** check novoms'
voms-proxy-info -all -file /tmp/x509up_u`id -u`_novoms
echo '************** voms-proxy-init'
voms-proxy-init -voms atlas:/atlas/usatlas/Role=production -valid 100000:0 -noregen -debug -cert /tmp/x509up_u`id -u`_novoms
echo '************** check new proxy'
voms-proxy-info -all
echo '************** end'
echo
