1. Configure your environment
export PANDA_URL="http://pandawms.org:25080/server/panda"
export PANDA_URL_SSL="https://pandawms.org:25443/server/panda"
voms-proxy-init -voms lsst:/lsst

2. Submit a job
python lsstSubmit.py
