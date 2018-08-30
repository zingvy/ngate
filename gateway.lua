local nats = require 'nats'
local cjson = require 'cjson'
local ck = require 'cookie'

local nats_host = "127.0.0.1"
local nats_port = 4222
local env = "production"


local c = nats:new()
local ok, err = c:connect(nats_host, nats_port)
if not ok then
    ngx.log(ngx.ERR, err)
    return
end

local count
local client = c:get_client()

count, err = c:get_reused_times()
if 0 == count then
    client:connect() 
elseif err then
    ngx.log(ngx.ERR, err)
end

-- request 
local uri = ngx.var.uri
uri = env .. string.gsub(uri, "/", ".")
local param = {}
for k, v in pairs(ngx.req.get_uri_args()) do
    param[k]=v
end
ngx.req.read_body()
for k, v in pairs(ngx.req.get_post_args()) do
    param[k]=v
end

-- client header
local header = {}
header["ip"] = ngx.var.remote_addr
header["ua"] = ngx.var.http_user_agent
param["HEADER"] = header 

-- cookie
local cookie, err = ck:new()
if not cookie then
    ngx.log(ngx.ERR, err)
    return
end

-- get all cookies
local fields, err = cookie:get_all()
if not err then
    local cookies = {}
    for k, v in pairs(fields) do
        cookies[k]=v
    end
    param["COOKIE"] = cookies
end

local callback = function(message, reply)
    if string.find(message, "COOKIE") == nil then
        ngx.say(message)
        return
    end
    local data = cjson.decode(message)
    local cks = data["COOKIE"]
    if cks ~= nil then
        for i, v in pairs(cks) do
            cookie:set({
                key = v["Name"],
                value = v["Value"],
                path = v["Path"],
                domain = v["Domain"],
                secure = v["Secure"],
                httponly = v["HttpOnly"],
                expires = v["Expires"], 
                max_age = v["MaxAge"],
                samesite = v["SameSite"]
            })
        end
        data["COOKIE"] = nil
    end
    ngx.say(cjson.encode(data))
end

client:request(uri, cjson.encode(param), callback)
-- client:request("nserver.hello.world", cjson.encode(param), callback)

local ok, err = c:set_keepalive(10000, 100)
if not ok then
    ngx.log(ngx.ERR, err)
    return
end
