Initially:
cd ~/panda
git clone https://github.com/PanDAWMS/panda-server.git
cd ~/panda/panda-server
git pull

Routinely:
cd ~/panda/panda-server/pandaserver/test/alice
source mysetup
cd test

grid-proxy-init
python titan_testScript_ec2_alice_2.py ANALY_ORNL_Titan

To cancel job:
python killJob.py <ID>

Monitor jobs here:
http://pandawms.org/bigpandamon/job/list/
http://pandawms.org/dev/jobs/
