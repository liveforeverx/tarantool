#!/usr/bin/env tarantool
os = require('os')

box.cfg{
    listen              = os.getenv("LISTEN"),
    slab_alloc_arena    = 0.1,
    pid_file            = "tarantool.pid",
    logger              = "| cat - >> tarantool.log",
    rows_per_wal        = 50,
    custom_proc_title   = "master",
}
require('console').listen(os.getenv('ADMIN'))

pcall(function()
    box.schema.user.grant('guest', 'replication')
end)

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

if box.space.counter == nil then
    local cnt_space = box.schema.create_space('counter')
    cnt_space:create_index('primary', {unique = true, parts = {1, 'STR'}})
end

counter = box.space.counter:select{'cnt'}[1]

if not counter then
    box.space.counter:insert{'cnt', 0}
end


function insert_tests(space)
    local right = box.space.counter:select{'cnt'}[1][2] + 1
    local left = right + 1
    for i = right, left do
        space:insert({i, 'test_data ' .. tostring(i), i * 2})
    end
    box.space.counter:replace{'cnt', left}
    return check(space)
end

