import optparse
import sys

# password
from pandaserver.config import panda_config
from pandaserver.taskbuffer.OraDBProxy import DBProxy

optP = optparse.OptionParser(conflict_handler="resolve")
optP.add_option("--user", action="store", dest="user", default=None, help="prodUserName")
optP.add_option(
    "--unban",
    action="store_const",
    const=True,
    dest="unban",
    default=False,
    help="unban the user",
)

options, args = optP.parse_args()

if options.user is None:
    print("--user=<prodUserName> is required")
    sys.exit(1)

proxyS = DBProxy()
proxyS.connect(panda_config.dbhost, panda_config.dbpasswd, panda_config.dbuser, panda_config.dbname)

prodUserName = sys.argv[1]

varMap = {}
varMap[":name"] = options.user
if options.unban:
    varMap[":status"] = None
else:
    varMap[":status"] = "disabled"

sql = "UPDATE ATLAS_PANDAMETA.users SET status=:status WHERE name=:name"

status, res = proxyS.querySQLS(sql, varMap)
if res is None:
    print("Failed with database error")
else:
    print(f"{res} rows updated")
