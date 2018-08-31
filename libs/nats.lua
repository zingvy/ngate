-- ### Library requirements ###

local cjson  = require('cjson')
local uuid   = require('uuid')


local tcp = ngx.socket.tcp
local setmetatable = setmetatable
local rawget = rawget


local _M = {}

local mt = { __index = _M }

_M._VERSION = '0.0.1'

_M._DESCRIPTION = 'LUA client for NATS messaging system. BUT only support request method https://nats.io'

-- ### Create a properly formatted inbox subject.

local function create_inbox()
    return '_INBOX.' .. uuid()
end

function _M.new(self)
    local sock, err = tcp()
    if not sock then
        return nil, err
    end
    return setmetatable({ _sock = sock }, mt)
end

function _M.set_timeout(self, timeout)
    local sock = rawget(self, "_sock")
    if not sock then
        return nil, "not initialized"
    end

    return sock:settimeout(timeout)
end

function _M.connect(self, ...)
    local sock = rawget(self, "_sock")
    if not sock then
        return nil, "not initialized"
    end

    uuid.seed()
    return sock:connect(...)
end

function _M.get_reused_times(self)
    local sock = rawget(self, "_sock")
    if not sock then
        return nil, "not initialized"
    end

    return sock:getreusedtimes()
end

function _M.set_keepalive(self, ...)
    local sock = rawget(self, "_sock")
    if not sock then
        return nil, "not initialized"
    end

    return sock:setkeepalive(...)
end

local function close(self)
    local sock = rawget(self, "_sock")
    if not sock then
        return nil, "not initialized"
    end

    return sock:close()
end
_M.close = close


local function _sock_read(sock)
    if len == nil then len = '*l' end
    local line, err = sock:receive(len)
    if not err then
        return line, nil
    else
	return nil, err
    end
end

local function _read_reply(sock)
    local payload, err = _sock_read(sock)
    if not payload then
        if err == "timeout" then
            sock:close()
        end
        return nil, err
    end

    local slices  = {}
    local data    = {}

    for slice in payload:gmatch('[^%s]+') do
        table.insert(slices, slice)
    end

    -- PING
    if slices[1] == 'PING' then
        data.action = 'PING'

    -- PONG
    elseif slices[1] == 'PONG' then
        data.action = 'PONG'

    -- MSG
    elseif slices[1] == 'MSG' then
        data.action    = 'MSG'
        data.subject   = slices[2]
        data.unique_id = slices[3]
        -- ask for line ending chars and remove them
        if #slices == 4 then
            data.content = _sock_read(sock, slices[4]+2):sub(1, -3)
        else
            data.reply   = slices[4]
            data.content = _sock_read(sock, slices[5]+2):sub(1, -3)
        end

    -- INFO
    elseif slices[1] == 'INFO' then
        data.action  = 'INFO'
        data.content = slices[2]

    -- INFO
    elseif slices[1] == '+OK' then
        data.action  = 'OK'

    -- INFO
    elseif slices[1] == '-ERR' then
        data.action  = 'ERROR'

   -- unknown type of reply
    else
	return nil, 'unknown response' .. payload
    end

    return data, nil
end

local function _raw_send(sock, buffer)
    local _, err = sock:send(buffer)
    return err
end

function _M.request(self, subject, payload)
    local sock = rawget(self, "_sock")
    if not sock then
        return nil, "not initialized"
    end

    local inbox = create_inbox()
    local unique_id = uuid()

    _raw_send(sock, 'SUB '..subject..' '..unique_id..'\r\n')
    self:publish(subject, payload, inbox)

    local data = self:read()
    
    _raw_send(sock, 'UNSUB '..unique_id..'\r\n')
    return data
end

function _M.publish(self, subject, payload, reply)
    local sock = rawget(self, "_sock")
    if not sock then
        return nil, "not initialized"
    end

    if reply ~= nil then
        reply = ' '..reply
    else
        reply = ''
    end
    _raw_send(sock, 'PUB '..subject..reply..' '..#payload..'\r\n'..payload..'\r\n')
end

function _M.read(self)
    local sock = rawget(self, "_sock")
    if not sock then
        return nil, "not initialized"
    end

    local count = 0
    repeat
        local data = _read_reply(sock)

        if data.action == 'PING' then
            _raw_send(sock, 'PONG\r\n')

        elseif data.action == 'MSG' then
            return data.content
        end
    until count > 0
end

return _M
