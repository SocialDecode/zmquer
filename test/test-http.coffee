httpserver = require '../lib/httpserver'
config = require '../config/defaults-server.json'
processing = {}

universe = [0]

callback = {}
callback.resetJobs = ()->
  cleanedProc = {}
  for k, v of processing
    cleanedProc[k] = v if v isnt 3
  processing = cleanedProc
  cleanedProc = null
  httpserver.syncData()

callback.syncData = (cb)->
  cb({processing:processing,universe:universe})

httpserver.startHttp {
  cfg:config
  callback : callback
}

i = 1
int = setInterval( ()->

  processing[i] = (i%3)+1
  i++

  #This is not how actually the universe of jobs is obtained
  #But it serves for testing purposes
  universe[0] = Object.keys(processing).length

  resume = {total:universe[0],pending:0,executing:0,withErr:0}
  for k, v of processing
    resume.executing++ if v is 2
    resume.withErr++ if v is 3
  resume.pending = resume.total - (resume.executing+resume.withErr)
  #console.log resume
,3000)