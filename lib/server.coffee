###*
*
* The Server
*
*
###
#testing for non-daemon monde
canprogress = true
refreshrate = 500
zmqWorking = 3
try
	process.stdout.clearLine()
catch err
	canprogress = false
	refreshrate = 5000

main = ->
	col = {}
	couch_processing = {}
	couch_errors = {}
	ref_errors = [ 0 ]
	jobsCounter = [ 0 ]
	httpserver = require('./httpserver')
	async = require('async')
	hostJobs = {}
	httpCB = 
		syncData: (cb) ->
			cb
				processing: couch_processing
				universe: jobsCounter
				servers: hostJobs
				errors: ref_errors
			return
		resetJobs: ->
			couch_errors = {}
			ref_errors[0] = 0
			httpserver.syncData()
			return
	nano = require('nano')
	jsonpack = require('jsonpack/main')
	os = require('os')
	couchdb = null
	couchque = null
	tickreset = 5
	justSent = {}
	zmq = require('zmq')
	s_wk = zmq.socket('push', _maxListeners: 50)
	s_wc = zmq.socket('push')
	s_wc_client = zmq.socket('pull', _maxListeners: 50)
	r = 
		'ready': false
		'listening': false
		'DBSyncInterval': null
		'initServer': ->
			workque = []
			maxquelen = 1000
			jumpon = 0
			findinque = (id)->
				for obj in workque
					if obj.id is id or obj._id is id
						return obj
				return false
			findplace = (id)->
				found = -1
				for obj,i in workque
					if obj.id is id or obj._id is id
						found = i
				return found
			dirtyqueue = 0
			async.forever((next)->
				unless r.ready
					setImmediate -> next()
				else
					jumpon++
					status_c = {}
					tokill = []
					for act in workque
						switch act._status
							when "new"
								act._status = "tofetch"
								act._lastchange = ~~((new Date).getTime() / 1000)
								getDocsCargo.push act.id
							when "tosend"
								act._status = "onqueue"
								act._lastchange = ~~((new Date).getTime() / 1000)
								dirtyqueue = ~~((new Date).getTime() / 1000)
								s_wk.send if config.jsonpack then jsonpack.pack(bulkbody.rows[i].doc) else JSON.stringify(act)
							when "completed"
								act._status = "todrop"
								act._lastchange = ~~((new Date).getTime() / 1000)
								couchDelCargo.push act
							when "dropped"
								tokill.push act._id
							when "droperr"
								act._status = "tofetch"
								act._lastchange = ~~((new Date).getTime() / 1000)
							# when "onqueue"
							# 	if s_wk._zmq.pending  is 0
							# 		# the que lost all elements .. zmq bug
							# 		act._status = "tosend"
							# 		act._lastchange = ~~((new Date).getTime() / 1000)
							when "errored"
								if act.exec?
									act._status = "tosend"
									act._lastchange = ~~((new Date).getTime() / 1000)
									act._retries ||= 0
									act._retries += 1
								else
									#it was a ghost job
									tokill.push act._id
						status_c[act._status] ||= 0
						status_c[act._status]++
					if tokill.length > 0
						workque = workque.filter (obj)->
							for item in tokill
								return false if obj._id is item
							return true
					if jumpon % refreshrate is 0
						output = []
						for status,count of status_c
							output.push "#{status} : #{count}"
						#console.log output.join(" | "), output.length,".--"
						#process.stdout.clearLine()
						output.push "zmq : " + s_wk._zmq.pending
						output.push "mem : " + (if os.freemem() < (config.minMem * 1048576) then "Ok" else "notOk")
						if canprogress
							process.stdout.cursorTo(60)
							process.stdout.write output.join(" | ")
							process.stdout.cursorTo(0)
						else
							console.log output.join(" | ")
						jumpon = 0
					
					# Inconsistency checks for zmq queue

					if ((~~((new Date).getTime() / 1000)) - dirtyqueue > zmqWorking) and status_c.onqueue? and status_c.onqueue isnt s_wk._zmq.pending
						console.log "Inconsistent Queue", status_c.onqueue, s_wk._zmq.pending
						zmqids = []
						for item,ix in s_wk._outgoing
							lid = JSON.parse(item[0].toString('utf-8'))._id
							zmqids.push([lid,ix])
						# On zmq but not on que
						notonque = []
						for item in zmqids
							notonque.push(item[1]) if findinque(item[0])._status isnt "onqueue" # not on que
						# Duplicates
						dups = []
						for item,ix in zmqids
							for inItem,inIx in zmqids
								dups.push(item[1]) if ix isnt inIx and item[0] is inItem[1] # duplicates
						console.log "NotOnQue", notonque.length, "Dups",dups.length
						todelete = notonque.concat(dups)
						if todelete.length isnt 0
							console.log "Removing", todelete.length,"items from zmqQueue", todelete
							s_wk._outgoing = s_wk._outgoing.filter (obj,ix)->
								return false if todelete.indexOf(ix) isnt -1
								return true
						# on Que but not on zmq
						for item in workque
							if item._status is "onqueue"
								found = false
								for zItem in zmqids
									found = true if zItem[0] is item._id
								if !found
									console.log "requeueing",item._id
									item._status = "tosend"
									item._lastchange = ~~((new Date).getTime() / 1000)
					setImmediate -> next()
			,(err)->
				#it will never stop
			)
			datalog = 
				gauge: ->
				legauge: ->
			#empty placeholder

			###*
			*
			* Set Up the Server that hosts the Ques
			*
			*
			###

			if r.ready
				return r
			#already initialized
			onReady = undefined
			if arguments and arguments.length > 0
				x = 0
				while x < arguments.length
					switch typeof arguments[x]
						when 'function'
							onReady = arguments[x]
						when 'object'
							if !arguments[x].done
								config = arguments[x]
					x++
				if onReady
					config.onReady = onReady
			else
				throw new Error('Not enough arguments !')
			if config.datadog
				statsD = require('node-statsd')
				datalog = new statsD
				datalog.socket.on 'error', (exception) ->
					console.log '[datadog] error event in socked.send():' + exception
					return
				datalog.legauges = {}

				datalog.legauge = (type, host) ->
					key = type + host
					now = ~ ~((new Date).getTime() / 1000)
					slot = Math.floor(now / 20) * 20 + ''
					if !datalog.legauges[key]
						datalog.legauges[key] = {}
					if !datalog.legauges[key][slot]
						datalog.legauges[key][slot] = 1
					else
						datalog.legauges[key][slot]++
					jobsDone = 0
					for ix of datalog.legauges[key]
						if now - parseInt(ix) > 300
							delete datalog.legauges[key][ix]
						else
							jobsDone += datalog.legauges[key][ix]
					datalog.gauge type, jobsDone, [ 'worker:' + host ]
					return

			startClients = ->
				# Setup zmq listening to push of commands (to clients)
				s_wk.bindSync 'tcp://*:' + config.port
				# Setup zmq for return data on the next port (from clients)
				s_wc_client.bindSync 'tcp://*:' + (config.port + 1)
				###==========  ZmQ listening  ==========###
				s_wc_client.on 'message', (msg) ->
					c = JSON.parse(msg)
					#set the lastseen flag and re-sync
					if c?.node?.name?
						if !hostJobs[c.node.name]
							hostJobs[c.node.name] = lastseen: ~~((new Date).getTime() / 1000)
						else
							hostJobs[c.node.name].lastseen = ~~((new Date).getTime() / 1000)
						syncJobs c.node.name, c.jobIds
						datalog.gauge 'task_processing', c.jobIds.length, [ 'worker:' + c.node.name ] if c.jobIds? and Array.isArray(c.jobIds)
					if c.id
						switch c.status
							when 2 # job taken
								findinque(c.id)?._status = "working"
								findinque(c.id)?._takenby = c.node.name if c.node?.name?
								findinque(c.id)?._lastchange = ~~((new Date).getTime() / 1000)
							when 3 # job finished
								if c.error
									# error on the job
									jobItem = findinque(c.id)
									if jobItem._status is "working" and jobItem._takenby is c.node.name
										jobItem._status = "errored"
										jobItem._lastchange = ~~((new Date).getTime() / 1000)
										jobItem.error = c.error
									console.log ':(', (if c.node then c.node.name else '?'), c.id, c.error.message
									datalog.legauge 'task_error', c.node.name
									if config.errorlog isnt false #error log
										require('fs').appendFile config.errorlog, JSON.stringify(
											id: c.id
											error: c.error
											timestamp: ~ ~((new Date).getTime() / 1000)) + '\n', (apenderr) ->
											if apenderr
												console.error 'Unable to append to log file', apenderr
											return
								else
									# ok done
									findinque(c.id)?._status = "completed"
									findinque(c.id)?._lastchange = ~~((new Date).getTime() / 1000)
									datalog.legauge 'task_completed', c.node.name
					else
						if c.startup
							console.log 'Server Reconnected ... ', c.startup
							# reset status of jobs assignet to server
							for act in workque
								if act._status is "working" and act._takenby is c.startup
									act._status = "tosend"
									act._lastchange = ~~((new Date).getTime() / 1000)
						else
							console.log 'Not sure what to do with ', c
					#datalog.gauge 'task_errors', Object.keys(couch_errors).length
					return
				r.ready = true
				return

			###==========  Setup DB Connection [CouchDB]  ==========###

			couchdb = nano(config.couch.server)
			couchdb.db.create config.couch.database, (err) ->
				if err and err.error != 'file_exists'
					console.error 'unable to create the que database', err.message
				else
					couchque = couchdb.db.use(config.couch.database)

					couchque.update = (obj, key, callback) ->
						couchque.get key, (error, existing) ->
							if !error
								newobj = existing
								for attrname of obj
									newobj[attrname] = obj[attrname]
								couchque.insert newobj, key, callback
							else
								console.log 'Unable to find job ', key
								callback new Error('Unable to find job'), ''
							return
						return
					startClients()
					getKeysCargo.push
						limit: config.readKeySize
						descending: 'true'
					if config.onReady
						config.onReady()

					###*

						TODO:
						- Re-Que all Jobs, right now we only process the
						first dbIncremental, rather than all (which should be done by batches)

					*
					###

					httpserver.startHttp
						cfg: config
						callback: httpCB
				return

			
			couchDelCargoWorking = false
			couchDelCargo = async.cargo(((tasks, callback) ->
				if couchDelCargoWorking
					setImmediate ->
						couchDelCargo.push tasks
						return
					callback()
				else
					couchDelCargoWorking = true
					label = 'dropping ' + tasks.length + ' job(s)'
					console.time label
					docs = []
					for item in tasks
						docs.push
							'_id': item._id
							'_deleted': true
							'_rev': item._rev
					couchque.bulk { docs: docs }, (err, resp) ->
						couchDelCargoWorking = false
						console.timeEnd label
						if err
							console.log 'error while dropping..', err
							setImmediate ->
								`var x`
								couchDelCargo.push tasks
								return
							callback()
						else
							for item in resp
								unless item.error?
									#console.log(":) ",item.id);
									findinque(item.id)?._status = "dropped"
									findinque(item.id)?._lastchange = ~~((new Date).getTime() / 1000)
								else
									console.log ':| ', item.id, item.error
									findinque(item.id)?._status = "droperr"
									findinque(item.id)?._lastchange = ~~((new Date).getTime() / 1000)
							callback()
						return
				return
			), config.delBatchsize)
			

			# Cargo to gett all pending keys it auto updates when is drained
			getKeysCargo = async.cargo(((tasks, callback) ->
				if !r.ready or workque.length > maxquelen
					setImmediate ->
						getKeysCargo.push tasks
						return
					callback()
				else
					for task in tasks
						console.time 'getting ' + task.limit + ' keys'
						couchque.list task, (geterr, getbody) ->
							`var x`
							if geterr
								getKeysCargo.push tasks
							else
								jobsCounter[0] = getbody.total_rows
								if getbody.rows and getbody.rows.length > 0
									for act in getbody.rows
										if !findinque(act.id)
											act._status = "new"
											act._lastchange =  ~~((new Date).getTime() / 1000)
											workque.push act
									#run itself again
									if getbody.rows.length < config.readKeySize
										setImmediate ->
											getKeysCargo.push
												limit: config.readKeySize
												descending: 'true'
											return
									else
										setImmediate ((gb) ->
											->
												getKeysCargo.push
													limit: config.readKeySize
													startkey: gb.rows[gb.rows.length - 1].key
													descending: 'true'
												return
										)(getbody)
								else
									setImmediate ->
										getKeysCargo.push
											limit: config.readKeySize
											descending: 'true'
										return
							callback()
							return
				return
			), 1)
			getDocsCargo = async.cargo(((couch_toprocess, callback) ->
				if !r.ready or os.freemem() < config.minMem * 1048576 or couchDelCargo.length() > config.delBatchsize or couchDelCargoWorking
					setImmediate ->
						getDocsCargo.push couch_toprocess
						return
					callback()
				else
					keys = []
					keys.push key for key in couch_toprocess
					label = 'getting ' + keys.length + ' docs'
					console.time label
					couchque.list {
						keys: keys
						include_docs: true
					}, (bulkerr, bulkbody) ->
						console.timeEnd label
						if bulkerr
							console.log 'error while getting DB data', bulkerr
							setImmediate ->
								getDocsCargo.push couch_toprocess
								return
							callback()
						else
							for item in bulkbody.rows
								if item?.doc?.exec?
									doc = item.doc
									doc._status = "tosend"
									doc._lastchange = ~~((new Date).getTime() / 1000)
									index = findplace(doc._id)
									if workque[index]._status is "tofetch"
										workque[index] = doc
									else
										#it was a gosth job
										console.log "updating info for ghost job", doc._id
										doc._status = workque[index]._status
										doc._takenby = workque[index]._takenby if workque[index]._takenby?
										doc._retries = workque[index]._retries if workque[index]._retries?
										workque[index] = doc
								else
									console.log "invalid doc",item?.id
						callback()
			), config.readBatchsize)
			syncJobs = (host, jobs) ->
				if host? and Array.isArray(jobs)
					# add all the jobs the client sent it already has
					# todo : we can't because we would not be able to erase them from db witouth the _rev

					# Job Pruning
					assigned = []
					assigned.push(item._id) for item in workque when item._status is "working" and item._takenby is host

					# Working job but not assigned 
					for job in jobs
						jobId = job[0]
						jobRv = job[1]
						found = false
						for item in assigned
							found = true if item is jobId
						if not found
							if findplace(jobId) isnt -1 # update the job
								currentjob = findinque(jobId)
								console.log "re-assign",jobId,currentjob._takenby, "->",host
								currentjob._takenby = host
								currentjob._status = "working"
								currentjob._lastchange = ~~((new Date).getTime() / 1000)
							else # create a ghosth job
								console.log "creating ghost job",jobId,"for", host
								workque.push {
									_id : jobId,
									_rev : jobRv,
									_status : "working",
									_lastchange : ~~((new Date).getTime() / 1000)
								}

					# Assigned jobs but not working
					for item in assigned
						found = false
						for job in jobs
							found = true if item is job[0] 
						if !found and item?# re-enqueu the job if not a ghost
							console.log "un-assign an assigned job", item, "to",host
							if findinque(item)?.exec?
								findinque(item)._status = "tosend"
								findinque(item)._lastchange = ~~((new Date).getTime() / 1000)
								delete findinque(item)._takenby
							else # kill the ghost job
								workque = workque.filter (obj)->
									return obj._id isnt item

				return
			return r
		'addJob': (command, callback) ->
			if !command.exec
				console.log 'No exec !', command
				callback false
				return false
			command._id = guid()
			command.created = (new Date).getTime() / 1000 | 0
			col.insert command, (addJ_err, addJ_doc) ->
				if addJ_err
					throw addJ_err
				callback addJ_doc[0]
				return
			return
		'listenJobs': (options) ->
			if r.listening
				return
			jobs = []
			s_wk_client = zmq.socket('pull')
			#socket Work Que Client;
			#setup the work returns
			s_wc.connect options.uriret
			s_wc.send JSON.stringify(
				startup: os.hostname()
				jobsIds: jobs)

			on_message = (msg) ->
				updateState()
				#one more job
				c = if options.jsonpack then jsonpack.unpack(String(msg)) else JSON.parse(msg)
				args = ''
				if !c.exec or jobs.indexOf(c._id) > -1
					#tell mothership we already had this ..
					if jobs.indexOf(c._id) > -1
						console.log 'already had ', c._id, ' :|'
						s_wc.send JSON.stringify(
							id: c._id
							status: 2
							idtype: c.idtype
							node:
								name: os.hostname()
								loadavg: os.loadavg()
								freemem: os.freemem()
								totamem: os.totalmem()
							jobIds: jobs)
					return
				jobs.push [c._id,c._rev]
				env = process.env
				env.jobId = c._id
				if !env.TMPDIR
					env.TMPDIR = '/var/tmp/'
				fs = require('fs')
				filePath = env.TMPDIR + c._id + '.json'
				console.log 'got job ', (c.exec.split("/").pop()).split(" ")[0], c._id, 'currently running', jobs.length
				#sent back teh status job
				s_wc.send JSON.stringify(
					id: c._id
					status: 2
					idtype: c.idtype
					node:
						name: os.hostname()
						loadavg: os.loadavg()
						freemem: os.freemem()
						totamem: os.totalmem()
					jobIds: jobs)
				for arg of c.args
					args += ' ' + arg + ' ' + c.args[arg]
				# preparing the command
				cmdpath = if options.basepath then options.basepath + '/' else ''
				cmdfin = ''
				execpaths = [
					'.js'
					'.coffee'
				]
				first = true
				c.exec.split(' ').forEach (el) ->
					add = false
					if !first
						execpaths.forEach (ec) ->
							if el.length > ec.length and el.substr(el.length - ec.length) == ec
								add = true
							return
					if add and !first
						cmdfin += ' ' + cmdpath + el
					else
						if first
							cmdfin += el
						else
							cmdfin += ' ' + el
					first = false
					return
				cmd = cmdpath + cmdfin + args
				#create a file for the arguments in env
				if c.env
					fs.writeFileSync filePath, JSON.stringify(c.env), 'utf-8'
				path = cmd.split('/')
				path.pop()
				path = path.join('/')
				env.PATH += ':' + path
				cmd = cmd.split(' ')
				cmd1 = cmd.shift()
				#separating command from arguments
				require('child_process').execFile cmd1, cmd, {
					cwd: path
					env: env
				}, (error, stdout, stderr) ->
					if c.env and fs.existsSync(filePath)
						fs.unlinkSync filePath
					#erasing input file
					if stdout
						console.log 'stdout', cmd, stdout
					if error
						console.log 'error', c._id, cmd1, cmd, error
					#check if there is a file output
					file_ret = if fs.existsSync(env.TMPDIR + c._id + '_out.json') then fs.readFileSync(process.env.TMPDIR + c._id + '_out.json', 'utf8') else null
					#sending back the status
					s_wc.send JSON.stringify(
						id: c._id
						idtype: c.idtype
						stdout: stdout
						error: error
						status: 3
						node:
							name: os.hostname()
							loadavg: os.loadavg()
							freemem: os.freemem()
							totamem: os.totalmem()
							jobs: String(jobs.length - 1)
						file: file_ret
						_rev: c._rev
						jobIds: jobs)
					#one less job
					jobs = jobs.filter (obj)->
						return true if obj[0] isnt c._id
					console.log 'job done', (c.exec.split("/").pop()).split(" ")[0], c._id, 'currently running', jobs.length

					###*

						TODO:
						- This Error Catch is for an error that is not recoverable

					*
					###

					if error and error.code == 'Unknown system errno 7' and options.err7
						#unrecoverable error
						console.log 'Unrecoverable error 7'
						process.exit 1
					updateState()
					return
				return

			#timed restart of job processing
			updateStateRunning = false

			updateState = ->
				if updateStateRunning
					return
				updateStateRunning = true
				if s_wk_client._zmq.state != zmq.STATE_BUSY
					#console.log(s_wk_client._zmq.state,jobs.length,options.max,freemem);
					switch s_wk_client._zmq.state
						when zmq.STATE_CLOSED
							if os.freemem() - options.memJob * 1048576 * jobs.length > options.minMem * 1048576
								console.log 'connecting...'
								s_wk_client = zmq.socket('pull')
								s_wk_client.on 'message', on_message
								s_wk_client.connect options.uri
						when zmq.STATE_READY
							if os.freemem() - options.memJob * 1048576 * jobs.length < options.minMem * 1048576
								console.log 'disconnecting...'
								s_wk_client.close()
								s_wk_client = '_zmq': 'state': zmq.STATE_CLOSED
				updateStateRunning = false
				return

			#initial setup
			s_wk_client.on 'message', on_message
			s_wk_client.connect options.uri
			setInterval updateState, 1000
			r.listening = true
			return
		'monitorJob': (jobId) ->

	###==========  Helpers  ==========###

	guid = do ->
		s4 = ->
			Math.floor((1 + Math.random()) * 0x10000).toString(16).substring 1
		return s4() + s4() + '-' + s4() + '-' + s4() + '-' + s4() + '-' + s4() + s4() + s4()
	return r

module.exports = main
