from taskbuffer.OraDBProxy import DBProxy
from config import panda_config

proxy = DBProxy()
proxy.connect(panda_config.dbhost, panda_config.dbpasswd, panda_config.dbuser, panda_config.dbname)

# get the distribution of job states by resource type
sql = """
      SELECT resource_type, jobstatus_grouped, SUM(HS)/1000000 MHS06
          FROM
              (SELECT resource_type, HS,
                   CASE
                       WHEN jobstatus IN('activated') THEN 'queued'
                       WHEN jobstatus IN('sent', 'running') THEN 'executing'
                       ELSE 'ignore'
                   END jobstatus_grouped
               FROM ATLAS_PANDA.JOBS_SHARE_STATS JSS)
          GROUP BY resource_type, jobstatus_grouped
      """

proxy.querySQL(sql)



print (proxyS.get_sorted_leaves())

    # print the global share structure
    print('--------------GLOBAL SHARES TREE---------------')
    print(proxyS.tree)

    # print the normalized leaves, which will be the actual applied shares
    print('--------------LEAVE SHARES---------------')
    print(proxyS.leave_shares)

    # print the current grid status
    print('--------------CURRENT GRID STATUS---------------')
    print(proxyS.tree.pretty_print_hs_distribution(proxyS._DBProxy__hs_distribution))