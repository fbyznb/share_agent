-- KEYS: stock, legacy buyers set, buyer->order mapping, reservation.
-- ARGV: orderId, skuId, userId, accepted (0/1), keepBuyer (0/1).
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

if #KEYS ~= 4 or #ARGV ~= 5 or not positive_long(ARGV[1])
        or not positive_long(ARGV[2]) or not positive_long(ARGV[3])
        or (ARGV[4] ~= '0' and ARGV[4] ~= '1')
        or (ARGV[5] ~= '0' and ARGV[5] ~= '1') then
    return redis.error_reply('Invalid reservation outcome arguments')
end

require_type(KEYS[1], 'string')
require_type(KEYS[2], 'set')
require_type(KEYS[3], 'string')
if require_type(KEYS[4], 'hash') ~= 'hash' then
    return redis.error_reply('Reservation missing; reconciliation required')
end
if redis.call('HGET', KEYS[4], 'skuId') ~= ARGV[2]
        or redis.call('HGET', KEYS[4], 'userId') ~= ARGV[3] then
    return redis.error_reply('Reservation owner mismatch')
end

local state = redis.call('HGET', KEYS[4], 'state')
local target = ARGV[4] == '1' and 'CONFIRMED' or 'CANCELLED'
if state == target then
    if target == 'CANCELLED' and redis.call('HGET', KEYS[4], 'keepBuyer') ~= ARGV[5] then
        return redis.error_reply('Conflicting cancellation outcome')
    end
    return 1
end
if state ~= 'PENDING' then
    return redis.error_reply('Conflicting or invalid reservation outcome')
end
if redis.call('GET', KEYS[3]) ~= ARGV[1] then
    return redis.error_reply('Reservation no longer owns buyer mapping')
end
if ARGV[4] == '1' then
    redis.call('HSET', KEYS[4], 'state', 'CONFIRMED')
    return 1
end

local stock = redis.call('GET', KEYS[1])
if not stock or (stock ~= '0' and not positive_long(stock))
        or stock == '9223372036854775807' then
    return redis.error_reply('Stock missing, invalid or overflowing; reconciliation required')
end

-- Restore a durable purchase marker for a duplicate user purchase. For a normal
-- cancellation, remove only this reservation's marker. Roll back local mutations
-- if a later command fails, so a retry cannot increment stock a second time.
local was_member = redis.call('SISMEMBER', KEYS[2], ARGV[3])
local restored = false
local changed_marker = false
local unmapped = false
local function checked(command, ...)
    local reply = redis.pcall(command, ...)
    if type(reply) == 'table' and reply.err then
        if unmapped then redis.call('SET', KEYS[3], ARGV[1]) end
        if changed_marker then
            if was_member == 1 then redis.call('SADD', KEYS[2], ARGV[3])
            else redis.call('SREM', KEYS[2], ARGV[3]) end
        end
        if restored then redis.call('DECR', KEYS[1]) end
        error(reply.err)
    end
    return reply
end

checked('INCR', KEYS[1])
restored = true
if ARGV[5] == '1' then checked('SADD', KEYS[2], ARGV[3])
else checked('SREM', KEYS[2], ARGV[3]) end
changed_marker = true
checked('DEL', KEYS[3])
unmapped = true
checked('HSET', KEYS[4], 'state', 'CANCELLED', 'keepBuyer', ARGV[5])
return 1
