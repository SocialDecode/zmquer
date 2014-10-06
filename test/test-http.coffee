httpserver = require '../lib/httpserver'
config = require '../config/defaults-server.json'
processing = {}
for i in [0...1000]
  processing[i] = i%4

universe = [1000]


httpserver.startHttp {
  cfg:config
  processing:processing
  universe:universe
}