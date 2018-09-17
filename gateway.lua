local nats = require 'nats'
local cjson = require 'cjson'
local ck = require 'cookie'

local nats_host = "127.0.0.1"
local nats_port = 4222
local env = "production"


local client = nats:new()
local ok, err = client:connect(nats_host, nats_port)
if not ok then
    ngx.log(ngx.ERR, err)
    return
end

local count
count, err = client:get_reused_times()
if err then
    ngx.log(ngx.ERR, err)
    return
end
ngx.say('reuse count:', count)

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

--client:request(uri, cjson.encode(param), callback)
local message = client:request("nserver.hello.world", cjson.encode(param))

if string.find(message, "COOKIE") == nil then
    ngx.say(message)
else
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
                expires = v["RawExpires"], 
                -- max_age = v["MaxAge"],
                samesite = (v["SameSite"] == 0 and "Lax") or "Strict" 
            })
        end
        data["COOKIE"] = nil
    end
    ngx.say(cjson.encode(data))
end

--ok, err = client:set_keepalive(10000, 100)
--if not ok then
--    ngx.say("set keep alive failed", err)
--    client:close()
--end
