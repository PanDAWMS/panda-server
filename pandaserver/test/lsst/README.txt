1. Configure your environment
export PYTHONPATH=$PWD/bigpanda-client-mysql-0.0.1:$PYTHONPATH
export PANDA_URL="http://pandawms.org:25080/server/panda"
export PANDA_URL_SSL="https://pandawms.org:25443/server/panda"
voms-proxy-init -voms lsst:/lsst


2. Submit a job
cd bigpanda-client-mysql-0.0.1/test
python lsstSubmit.py
