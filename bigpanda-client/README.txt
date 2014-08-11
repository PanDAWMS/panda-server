BigPanDA:MySQL client for PanDA job submission

1] Download the client tarball
# wget http://pandawms.org/bigpanda-client/bigpanda-client-mysql-0.0.1.tar.gz


2] Unpack the tarball
# tar zxfv bigpanda-client-mysql-0.0.1.tar.gz


3] Read the README.txt file
# cd bigpanda-client-mysql-0.0.1
# less README.txt


4] Configure your environment
# export PYTHONPATH=$PWD/bigpanda-client-mysql-0.0.1:$PYTHONPATH
# export PANDA_URL="http://pandawms.org:25080/server/panda"
# export PANDA_URL_SSL="https://pandawms.org:25443/server/panda"
# voms-proxy-init -voms lsst:/lsst


5] Submit a job
# cd bigpanda-client-mysql-0.0.1/test
# python lsstSubmit.py


