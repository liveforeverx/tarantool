import os

engines = ['memtx', 'sophia']

# for each engine insert test data
command = 'insert_tests(box.space.%s_space)'
results = dict((e, server.admin(command % e)) for e in engines)
 
# make snapshot
server.admin('box.snapshot()')

# stop and start server
print 'Crashing tarantool'
server.stop()

print 'Start tarantool again'
server.start()

# check data
for engine in engines:
    check = server.admin('check(box.space.%s_space)' % engine)
    print check == results[engine]

