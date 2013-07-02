from config import panda_config
from config import panda_config_new

for item in dir(panda_config):
    if item.startswith('__'):
        continue
    old = getattr(panda_config,item)
    if not hasattr(panda_config_new,item):
        print "NG : %s not found" % item
        continue
    new = getattr(panda_config_new,item)
    if old != new:
        print "NG : %s missmatch" % item
        print "   old:%s" % old
        print "   new:%s" % new
