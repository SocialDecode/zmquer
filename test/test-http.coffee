httpserver = require '../lib/httpserver'
config = require '../config/defaults-server.json'
processing = {}

universe = [0]
servers = {}

couch_errors = {}
ref_errors = [0]

callback = {}
callback.resetJobs = ()->
  couch_errors = {}
  httpserver.syncData()

callback.syncData = (cb)->
  cb({
    processing : processing,
    universe : universe,
    servers : servers,
    errors : ref_errors
  })
  

httpserver.startHttp {
  cfg:config
  callback : callback
}

i = 1
int = setInterval( ()->

  processing[i] = ~~(Math.random()*3)+1

  for k in [1..14]
    servers['dummy'+k+'.socialdecode.com'] = {}
    server = servers['dummy'+k+'.socialdecode.com']
    server.lastseen = ~~(new Date().getTime()/1000)
    for j in [1..~~(Math.random()*10)+2]
      server[i] = ~~(new Date().getTime()/1000)

  i++

  #This is not how actually the universe of jobs is obtained
  #But it serves for testing purposes
  universe[0] = Object.keys(processing).length

  for k, v of processing
    couch_errors[new Date().getTime()] = true if v is 3

  ref_errors[0] = Object.keys(couch_errors).length
  
,1000)