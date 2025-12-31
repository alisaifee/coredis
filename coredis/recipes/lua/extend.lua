-- KEYS[1] - lock name
-- ARGS[1] - token
-- ARGS[2] - additional milliseconds
-- return 1 if the locks time was extended, otherwise 0

local token = redis.call('get', KEYS[1])
if not token or token ~= ARGV[1] then
    return 0
end
local expiration = redis.call('pttl', KEYS[1])
if not expiration then
    expiration = 0
end
if expiration < 0 then
    return 0
end
redis.call('pexpire', KEYS[1], expiration + ARGV[2])
return 1
