from DBMSql import DBMSql
import panda_proxy_cache
import os
import datetime

db = None

def makeMemoryCache(query):
    myconn = initDBAccess('oracle');
    mydata = myconn.executeQuery(query)
    return mydata

def initDBAccess(db_type='') :
    global db
    if db is None :
        DB_TYPE = db_type
        db = DBMSql(DB_TYPE)
    return db


if __name__ == '__main__' :

    print 'start at {0}'.format(datetime.datetime.utcnow())
    dbquery = 'select distinct  PRODUSERID from atlas_panda.JOBSDEFINED4 where MODIFICATIONTIME >= systimestamp-1 and CLOUD != '+" 'OSG' "+' and VO not in ( '+" 'cms' "+' , '+" 'CMS' "+' ) \
            union ALL \
    select distinct PRODUSERID  \
    from atlas_panda.JOBSWAITING4 where MODIFICATIONTIME >= systimestamp-1 and CLOUD != '+" 'OSG' "+' and VO not in ( '+" 'cms' "+' , '+" 'CMS' "+' )'

    userData = makeMemoryCache(dbquery)
    userList = []
    if userData != []:
        for i in userData:
            userList.append(str(i['PRODUSERID']).replace('/CN=proxy',''))
        #print userList
	print 'We need to get %s certs'% len(userList)
    my_proxy_interface_instance = panda_proxy_cache.MyProxyInterface()
    #for i in userList:
	#if '/CN=' not in i: # managed jobs have the user's email in the PRODUSERID instead of the UserDN
		# Production job - We need to use the ddm admin proxy
		#my_proxy_interface_instance.checkProxy('/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=ddmadmin/CN=531497/CN=Robot: ATLAS Data Management', production=True)
	#else:
	    #	my_proxy_interface_instance.checkProxy(i)
    userList = ['/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=gangarbt/CN=722147/CN=Robot: Ganga Robot']

    for i in userList:
	notify = my_proxy_interface_instance.checkProxy(i)
	my_proxy_interface_instance.checkProxy(i, production=False)	

	print 'notify is.. %s ' % notify
	#if notify != 0:
		#print 'will need to check db table to find the email of the user'
		#userDN = str(i + '/CN=proxy')
		#notifydbquery = '''SELECT email from ATLAS_PANDAMETA.users where "DN" = \'''' + userDN + '\' and email not null and email != '+" 'notsend' "+' '''
		#userEmail = makeMemoryCache(notifydbquery)
		#if userEmail != []:
		#	print 'user email is ... %s ' % userEmail
		#else:
		#	print 'user email is not in the db table...'

#    my_proxy_interface_instance.checkProxy('/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=scampana/CN=531497/CN=Simone Campana')
#    my_proxy_interface_instance.checkProxy('/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=scampana/CN=531497/CN=Simone Campana', production=True)	
#    my_proxy_interface_instance.checkProxy('/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=girolamo/CN=614260/CN=Alessandro Di Girolamo')
#    my_proxy_interface_instance.checkProxy('/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=girolamo/CN=614260/CN=Alessandro Di Girolamo', production=True)
#    my_proxy_interface_instance.checkProxy('/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=gangarbt/CN=703216/CN=Robot: Ganga Robot')
    print 'end at {0}'.format(datetime.datetime.utcnow())
    print
