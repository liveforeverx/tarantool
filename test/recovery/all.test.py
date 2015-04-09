import os
import time
from lib.tarantool_server import TarantoolServer

engines = ['memtx', 'sophia']
master = server

# for each engine insert test data
command = 'insert_tests(box.space.%s_space)'
results = dict((e, master.admin(command % e)) for e in engines)
 
# make snapshot
master.admin('box.snapshot()')

print 'Join replica'
master_id = master.get_param('server')['id']
lsn = master.get_lsn(master_id)
replica = TarantoolServer(master.ini)
replica.script = 'recovery/replica.lua'
replica.vardir = os.path.join(master.vardir, 'replica')
replica.rpl_master = master
replica.deploy()
replica.wait_lsn(master_id, lsn)

replica.admin('box.snapshot()')
nodes = [master, replica]

print '---------------[Starting crash test loop]----------------'
for i in xrange(0, 10):
    # stop and start nodes
    print 'Crashing tarantool'
    for node in nodes:
        node.stop()

    print 'Start tarantool again'
    for node in nodes:
        node.start()
    master_id = master.get_param('server')['id']
    lsn = master.get_lsn(master_id)
    #replica.admin('require("fiber").info()')
    replica.wait_lsn(master_id, lsn)
    
    # check data
    for node in nodes:
        node.admin('box.info().server.id')
        for engine in engines:
            check = node.admin('check(box.space.%s_space)' % engine)
            print check == results[engine]
    results = dict((e, master.admin(command % e)) for e in engines)
    print '---------------[End of crash iteration %d]----------------' % (i + 1)
