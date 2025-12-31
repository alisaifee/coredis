-- KEYS[1] - lock name
-- ARGS[1] - token
-- return 1 if the lock was released, otherwise 0

local token = redis.call('get', KEYS[1])
if not token or token ~= ARGV[1] then
    return 0
end
redis.call('del', KEYS[1])
return 1
