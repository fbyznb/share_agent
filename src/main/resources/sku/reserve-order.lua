-- KEYS: stock, legacy buyers set, buyer->order mapping, reservation, delivery stream.
-- ARGV: orderId, skuId, userId, maximum stream backlog. IDs must stay strings:
-- Lua numbers cannot represent every Java long exactly.
local function positive_long(value)
    return value and string.match(value, '^[1-9]%d*$')
        and (#value < 19 or (#value == 19 and value <= '9223372036854775807'))
end

local function require_type(key, expected)
    local actual = redis.call('TYPE', key).ok
    if actual ~= 'none' and actual ~= expected then
        error('Unexpected Redis key type: expected ' .. expected)
    end
    return actual
end

if #KEYS ~= 5 or #ARGV ~= 4 or not positive_long(ARGV[1])
        or not positive_long(ARGV[2]) or not positive_long(ARGV[3])
        or not positive_long(ARGV[4]) or #ARGV[4] > 10
        or (#ARGV[4] == 10 and ARGV[4] > '2147483647') then
    return redis.error_reply('Invalid reservation arguments')
end

require_type(KEYS[1], 'string')
require_type(KEYS[2], 'set')
require_type(KEYS[3], 'string')
local reservation_type = require_type(KEYS[4], 'hash')
require_type(KEYS[5], 'stream')

-- Validate existing IDs before any writes, including accidental cross-instance collisions.
if reservation_type == 'hash' then
    if redis.call('HGET', KEYS[4], 'skuId') ~= ARGV[2]
            or redis.call('HGET', KEYS[4], 'userId') ~= ARGV[3] then
        return redis.error_reply('Order ID collision')
    end
end

local previous = redis.call('GET', KEYS[3])
if previous then
    if not positive_long(previous) then
        return redis.error_reply('Invalid buyer order mapping')
    end
    local previous_key = 'sku:order:reservation:' .. previous
    if require_type(previous_key, 'hash') ~= 'hash'
            or redis.call('HGET', previous_key, 'skuId') ~= ARGV[2]
            or redis.call('HGET', previous_key, 'userId') ~= ARGV[3] then
        return redis.error_reply('Buyer mapping has no matching reservation')
    end
    local state = redis.call('HGET', previous_key, 'state')
    if state ~= 'PENDING' and state ~= 'CONFIRMED' then
        return redis.error_reply('Buyer mapping references an inactive reservation')
    end
    return previous
end

-- Never reuse even a cancelled order ID: old deliveries may still carry that ID.
if reservation_type ~= 'none' then
    return redis.error_reply('Order ID already used without an active buyer mapping')
end
if redis.call('SISMEMBER', KEYS[2], ARGV[3]) == 1 then
    return ''
end

local stock = redis.call('GET', KEYS[1])
if not stock then
    return ''
end
if stock ~= '0' and not positive_long(stock) then
    return redis.error_reply('Stock must be a nonnegative canonical 64-bit integer')
end
if stock == '0' or redis.call('XLEN', KEYS[5]) >= tonumber(ARGV[4]) then
    return ''
end

-- Lua serializes execution but does not roll back runtime errors. Validate predictable
-- failures above and undo our writes if any command (in particular XADD) reports one.
local deducted = false
local marked = false
local mapped = false
local recorded = false
local function checked(command, ...)
    local reply = redis.pcall(command, ...)
    if type(reply) == 'table' and reply.err then
        if recorded then redis.call('DEL', KEYS[4]) end
        if mapped then redis.call('DEL', KEYS[3]) end
        if marked then redis.call('SREM', KEYS[2], ARGV[3]) end
        if deducted then redis.call('INCR', KEYS[1]) end
        error(reply.err)
    end
    return reply
end

checked('DECR', KEYS[1])
deducted = true
checked('SADD', KEYS[2], ARGV[3])
marked = true
checked('SET', KEYS[3], ARGV[1])
mapped = true
checked('HSET', KEYS[4], 'skuId', ARGV[2], 'userId', ARGV[3], 'state', 'PENDING')
recorded = true
checked('XADD', KEYS[5], '*', 'orderId', ARGV[1], 'skuId', ARGV[2], 'userId', ARGV[3])
return ARGV[1]
