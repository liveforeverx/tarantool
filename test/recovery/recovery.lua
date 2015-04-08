#!/usr/bin/env tarantool
os = require('os')

box.cfg{
    listen              = os.getenv("LISTEN"),
    slab_alloc_arena    = 0.1,
    pid_file            = "tarantool.pid",
    rows_per_wal        = 50
}
require('console').listen(os.getenv('ADMIN'))

test_config = {
    memtx='memtx_space',
    sophia='sophia_space'
}

-- create spaces for engines defined in test_config
for engine_name, space_name in pairs(test_config) do
    if box.space[space_name] == nil then
        local new_space = box.schema.create_space(space_name, {engine=engine})
        new_space:create_index('primary', {unique = true, parts = {1, 'NUM'}})
    end
end

function check(space)
    return space:select{}
end

function insert_tests(space)
    for i = 1, 10 do
        space:insert({i, 'test_data ' .. tostring(i), i * 2})
    end
    return check(space)
end

