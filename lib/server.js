

/**
*
* The Server
*
**/

var main = function(){
	var col = {},
		couch_processing = {},
		couch_errors = {},
		ref_errors = [0],
		jobsCounter = [0],
		httpserver = require('./httpserver'),
		async = require("async"),
		hostJobs = {},
		httpCB = {
			syncData : function(cb){
				cb({
					processing:couch_processing,
					universe:jobsCounter,
					servers : hostJobs,
					errors : ref_errors
				});
			},
			resetJobs : function(){
				couch_errors = {};
				ref_errors[0] = 0;
				httpserver.syncData();
			}
		},
		nano     = require('nano'),
		jsonpack = require('jsonpack/main'),

		//Server side requirements
		os       = require("os"),
		couchdb  = null,
		couchque = null,
		tickreset = 5,
		justSent = {},
		zmq      = require('zmq'),
		s_wk = zmq.socket('push',{_maxListeners:50}), //socket Work Que

		s_wc = zmq.socket('push'), //socket Work CallBack
		s_wc_client = zmq.socket('pull',{_maxListeners:50}), //socket Work CallBack Client

		r = {
			"ready" : false,
			"listening" : false,
			"DBSyncInterval" : null,
			"initServer" : function (){
				/**
				*
				* Set Up the Server that hosts the Ques
				*
				**/
				if (r.ready) return r; //already initialized
				var onReady;
				if (arguments && arguments.length>0){
					for (var x=0;x<arguments.length;x++){
						switch (typeof arguments[x]){
							case "function" : 
								onReady = arguments[x];
								break;
							case "object" : 
								if (!arguments[x].done) config = arguments[x];
						}
					}
					if (onReady) config.onReady = onReady;
				}else{
					throw new Error("Not enough arguments !");
				}

				
				var startClients = function(){
					//Setup zmq listening to push of commands (to clients)
					s_wk.bindSync('tcp://*:'+config.port);
					//Setup zmq for return data on the next port (from clients)
					s_wc_client.bindSync('tcp://*:'+(config.port+1));
				};
				/*==========  Setup DB Connection [CouchDB]  ==========*/
				couchdb = nano(config.couch.server);
				couchdb.db.create(config.couch.database,function(err){
					if (err && err.error != 'file_exists'){
						console.error(
							'unable to create the que database',
							err.message
						);
					}else{
						couchque = couchdb.db.use(config.couch.database);
						couchque.update = function(obj, key, callback) {
							couchque.get(key, function (error, existing) {
								if(!error){
									var newobj = existing;
									for (var attrname in obj) { newobj[attrname] = obj[attrname]; }
									couchque.insert(newobj,key, callback);
								}else{
									console.log("Unable to find job ",key);
									callback(new Error ("Unable to find job"),"");
								}
							});
						};
						r.ready = true;
						startClients();
						getKeysCargo.push({limit:config.readKeySize,descending:"true"});
						if (config.onReady) config.onReady();
						/**
						
							TODO:
							- Re-Que all Jobs, right now we only process the
							first dbIncremental, rather than all (which should be done by batches)
						
						**/
						httpserver.startHttp({
							cfg      : config,
							callback : httpCB
						});
						
					}
				});

				/*==========  ZmQ listening  ==========*/
				var couchDelCargoWorking = false;
				var couchDelCargo =  async.cargo(function (tasks, callback){
					if (couchDelCargoWorking){
						setImmediate(function(){couchDelCargo.push(tasks);});
						callback();
					}else{
						couchDelCargoWorking = true;
						var label = "dropping "+tasks.length+" job(s)";
						console.time(label);
						var docs = [];
						for (var i=0;i<tasks.length;i++){
							docs.push({
								"_id" : tasks[i].id,
								"_deleted" : true,
								"_rev" : tasks[i].c._rev
							});
						}
						couchque.bulk({docs:docs},function(err,resp){
							couchDelCargoWorking = false;
							console.timeEnd(label);
							if (err){
								console.log("error while dropping..",err);
								setImmediate(function(){couchDelCargo.push(tasks);});
								callback();
							}else{
								for (var x=0;x<resp.length;x++){
									if (!resp[x].error){
										//console.log(":) ",resp[x].id);
									}else{
										console.log(":| ",resp[x].id,resp[x].error);
									}
									delete couch_processing[resp[x].id];
									delete couch_errors[resp[x].id];
									ref_errors[0] = Object.keys(couch_errors).length;
								}
								callback();
							}
						});
					}
				},config.delBatchsize);
				s_wc_client.on('message', function(msg){
					var c = JSON.parse(msg);
					//set the lastseen flag
					if (c && c.node && c.node.name){
						if (!hostJobs[c.node.name]) {
							hostJobs[c.node.name] = {
								lastseen : (~~((new Date()).getTime()/1000))
							};
						}else{
							hostJobs[c.node.name].lastseen = ~~((new Date()).getTime()/1000);
						}
					}
					if (c.id){
						var now = ~~((new Date()).getTime()/1000);
						var id = (c.idtype == "string" ? c.id : ObjectID(c.id));
						delete c.id;
						if (c.node.name && hostJobs[c.node.name]){
							if (c.status != 3){
								hostJobs[c.node.name][id] = now;
							}else{
								delete hostJobs[c.node.name][id];
							}
						}else if (c.node.name && c.status != 3){
							//the node took the job and it is not an end
							hostJobs[c.node.name][id] = now;
						}
						if (c.node.jobs && c.node.jobs==="0" && hostJobs[c.node.name] && Object.keys(hostJobs[c.node.name]).length > 0){
							//the node has orphan jobs
							for (var key in hostJobs[c.node.name]){
								if (key != "lastseen"){
									delete hostJobs[c.node.name][key];
									delete couch_processing[key];
								}
							}
						}
						var doUpdate = function(){
							couch_processing[id] = c.status;
							if (c.status==3 && c.error){
								console.log(":(",c.node?c.node.name:"?",id,c.error.message);
								if (couch_errors[id]) {
									couch_errors[id]++;
								}else{
									couch_errors[id] = 1;
								}
								ref_errors[0] = Object.keys(couch_errors).length;
								if (couch_errors[id] < config.retries) {
									delete couch_processing[id];
								}else{
									//error log
									if (config.errorlog){
										require("fs").appendFile(
											config.errorlog,
											JSON.stringify({id:id,error:c.error,timestamp:~~(new Date().getTime()/1000)})+"\n",
											function (apenderr) {
												if (apenderr) console.error("Unable to append to log file",apenderr);
											});
									}
								}
							}
						};
						if (config.dumpSuccess && c.status == 3 && !c.error){
							couch_processing[id] = 3;
							couchDelCargo.push({c:c,id:id});
						}else{
							doUpdate();
						}
						
					}else{
						if (c.startup){
							console.log("Server Reconnected ... ",c.startup);
							//erase all previous jobs from server
							for (var nodeJob in hostJobs[c.startup]){
								if (nodeJob !=="lastseen"){
									delete hostJobs[c.startup][nodeJob];
									delete couch_processing[nodeJob];
								}
							}
							/**
							
								TODO:
								- If the client deamon restart the jobs might still be running, need 
								to decide how to solve the issue first.
								- The best option is to chain the call back of the job finish to
								the job execution ?
							
							**/
						}else{
							console.log("Not sure what to do with ",c);
						}
						
					}
				});
			/*==========  DB Syncing  ==========*/
			/**
			*
			* Functions that Monitors the DB for Jobs and adds them (or remove's them from the que)
			*
			**/
			// Cargo to gett all pending keys it auto updates when is drained
				var couch_cargo = {};
				var getKeysCargo = async.cargo(function (tasks, callback){
					if (!r.ready  || getDocsCargo.length()>config.readBatchsize || couchDelCargo.length() > config.delBatchsize || couchDelCargoWorking) {
						setImmediate(function(){getKeysCargo.push(tasks);});
						callback();
					}else{
						for(var i=0; i<tasks.length; i++){
							var task = tasks[i];
							console.time("getting "+task.limit+" keys");
							couchque.list(task,function(geterr,getbody){
								if (geterr){
									getKeysCargo.push(tasks);
								}else{
									jobsCounter[0] = getbody.total_rows;
									if (getbody.rows && getbody.rows.length>0){
										//removing all keys alredy in que
										var couch_toprocess = [];
										for (var x=0;x<getbody.rows.length;x++){
											if (!couch_processing[getbody.rows[x].id] && !couch_cargo[getbody.rows[x].id]){
												couch_toprocess.push([getbody.rows[x].id,getbody.rows[x].value.rev]);
												couch_cargo[getbody.rows[x].id] = true;
											}
										}
										if (couch_toprocess.length>0){ //sending workers
											console.timeEnd("getting "+config.readKeySize+" keys");
											console.log("["+s_wk._zmq.pending+"."+Object.keys(couch_processing).length+"]q->"+couch_toprocess.length);
											getDocsCargo.push(couch_toprocess);
										}
										//run itself again
										if (getbody.rows.length<config.readKeySize){
											setImmediate(function () {
												getKeysCargo.push({limit: config.readKeySize,descending:"true"});
											});
										}else{
											setImmediate(
												(function(gb) {
													return function () {
														getKeysCargo.push({
															limit: config.readKeySize,
															startkey :gb.rows[gb.rows.length - 1].key,
															descending:"true"
														});
													};
												})(getbody)
											);
										}
									}else{
										setImmediate(function(){getKeysCargo.push({limit:config.readKeySize,descending:"true"});});
									}
								}
								callback();
							});
						}
					}
				},1);
				var getDocsCargo = async.cargo(function(couch_toprocess, callback){
					if (!r.ready  || os.freemem() < config.minMem*1048576 || couchDelCargo.length() > config.delBatchsize || couchDelCargoWorking) {
						setImmediate(function(){
							getDocsCargo.push(couch_toprocess);
						});
						callback();
					}else{
						var keys = [], revs = {};
						for (var y=0;y<couch_toprocess.length;y++) {
							keys.push(couch_toprocess[y][0]);
							revs[couch_toprocess[y][0]] = couch_toprocess[y][1];
						}
						var label = "getting "+keys.length+" docs";
						console.time(label);
						couchque.list({
							keys:keys,
							include_docs : true
						},function(bulkerr,bulkbody){
							console.timeEnd(label);
							if (bulkerr) {
								console.log("error while getting DB data",bulkerr);
								setImmediate(function(){
									getDocsCargo.push(couch_toprocess);
								});
								callback();
							}
							
							for (var value in justSent){if (justSent[value]>tickreset) {delete justSent[value];}justSent[value]++;} //already timedout

							for (var i=0;i<bulkbody.rows.length;i++){
								if (!bulkbody.rows[i].doc || !bulkbody.rows[i].doc.exec) continue;
								var id = bulkbody.rows[i].doc._id;
								couch_processing[id] = 1;
								bulkbody.rows[i].doc["_rev"] = revs[id];
								bulkbody.rows[i].doc["idtype"] = "string";
								s_wk.send(config.jsonpack ? jsonpack.pack(bulkbody.rows[i].doc) : JSON.stringify(bulkbody.rows[i].doc));
								if (!justSent[id])justSent[id] = 1; //increment the waiting on one ir set it as one
							}
							console.log("["+s_wk._zmq.pending+"]<-d"+bulkbody.rows.length);
							//Reset jobs when the queue is 0
							if(s_wk._zmq.pending===0){
								for (var jobId in couch_processing){
									if(couch_processing[jobId]===1 && !justSent[jobId]) delete couch_processing[jobId];
								}
							}
							callback();
						});
					}
					
				},config.readBatchsize);
				getDocsCargo.drain = function(){
					couch_cargo = {}; //reset
				};
				return r;
			},

			/*==========  Adding a Job  ==========*/

			/**
			*
			*	This Method is not to be used other than Unit Testing; 
			*	since this is ment to work solely from the DB collection
			*
			**/

			"addJob" : function(command, callback){
				if (!command.exec) {
					console.log("No exec !", command);
					callback(false);
					return false;
				}
				command._id = guid();
				command.created = ((new Date()).getTime())/ 1000 | 0;
				col.insert(command, function(addJ_err, addJ_doc) {
					if (addJ_err) throw addJ_err;
					callback(addJ_doc[0]);
				});
			},

			/*==========  Processing a Job  ==========*/

			"listenJobs" : function(options){
				if (r.listening) return;
				var jobs = [],
					s_wk_client = zmq.socket('pull'); //socket Work Que Client;
				//setup the work returns
				s_wc.connect(options.uriret);
				s_wc.send(JSON.stringify({startup : os.hostname()}));
				var on_message = function(msg){
					updateState();
					//one more job
					var c = options.jsonpack ? jsonpack.unpack(String(msg)) : JSON.parse(msg),
						args = "";
					if (!c.exec || jobs.indexOf(c._id)>-1/* || c.exec.indexOf("mysql")<0*/){
						//tell mothership we already had this ..
						if (jobs.indexOf(c._id)>-1){
							console.log ("already had ", c._id," :|")
							s_wc.send(JSON.stringify({
								id : c._id,
								status : 2,
								idtype : c.idtype,
								node : {
									name : os.hostname(),
									loadavg : os.loadavg(),
									freemem : os.freemem(),
									totamem : os.totalmem()
								}
							}));
						}
						return;
					}
					jobs.push(c._id);
					var env = process.env;
					env.jobId = c._id;
					if (!env.TMPDIR) env.TMPDIR = "/var/tmp/";
					var fs = require('fs'),
						filePath = env.TMPDIR + c._id + '.json';
					console.log("got job ",c._id,"currently running",jobs.length);
					//sent back teh status job
					s_wc.send(JSON.stringify({
						id : c._id,
						status : 2,
						idtype : c.idtype,
						node : {
							name : os.hostname(),
							loadavg : os.loadavg(),
							freemem : os.freemem(),
							totamem : os.totalmem()
						}
					}));
					for (var arg in c.args){
						args+=" " + arg + " "+ c.args[arg];
					}
					// preparing the command
					var cmdpath = (options.basepath ? options.basepath + "/" : "");
					var cmdfin = "";
					var execpaths = [".js",".coffee"];
					var first = true;
					c.exec.split(" ").forEach(function(el){
						var add = false;
						if (!first){
							execpaths.forEach(function(ec){
								if (el.length > ec.length && el.substr(el.length-ec.length)==ec){
									add = true;
								}
							});
						}
						if (add && !first){
							cmdfin += " " + cmdpath + el;
						}else{
							if (first){
								cmdfin += el;
							}else{
								cmdfin += " " + el;
							}
						}
						first = false;
					});
					var cmd = cmdpath + cmdfin + args;
					
					//create a file for the arguments in env
					if (c.env) fs.writeFileSync( filePath, JSON.stringify(c.env), 'utf-8');
					var path = cmd.split("/");path.pop();path = path.join("/");
					env.PATH +=":"+path;
					cmd = cmd.split(" ");var cmd1 = cmd.shift(); //separating command from arguments
					require('child_process').execFile(cmd1,cmd, {
						cwd : path,
						env : env
					} ,function(error, stdout, stderr) {
						if (c.env && fs.existsSync(filePath)) fs.unlinkSync(filePath); //erasing input file
						if (stdout) console.log("stdout",cmd,stdout);
						if (error) console.log("error",c._id,cmd1,cmd,error);
						//check if there is a file output
						var file_ret = (fs.existsSync(env.TMPDIR+c._id+"_out.json")) ? fs.readFileSync(process.env.TMPDIR+c._id+"_out.json", "utf8") : null;
						//sending back the status
						s_wc.send(JSON.stringify({
							id : c._id,
							idtype : c.idtype,
							stdout : stdout,
							error : error,
							status : 3,
							node : {
								name : os.hostname(),
								loadavg : os.loadavg(),
								freemem : os.freemem(),
								totamem : os.totalmem(),
								jobs : String((jobs.length)-1)
							},
							file:file_ret,
							_rev : c._rev
						}));
						//one less job
						if (jobs.indexOf(c._id)>-1) jobs.splice(jobs.indexOf(c._id),1);
						console.log("job done",c._id, "currently running",jobs.length);
						/**
						
							TODO:
							- This Error Catch is for an error that is not recoverable
						
						**/
						if (error && error.code === 'Unknown system errno 7' && options.err7){
							//unrecoverable error
							console.log("Unrecoverable error 7");
							process.exit(1);
						}
						updateState();
					});
				};
				//timed restart of job processing
				var updateStateRunning = false;
				var updateState = function(){
					if (updateStateRunning) return;
					updateStateRunning = true;
					if (s_wk_client._zmq.state !== zmq.STATE_BUSY){
						//console.log(s_wk_client._zmq.state,jobs,options.max,freemem);
						switch (s_wk_client._zmq.state){
							case zmq.STATE_CLOSED:
								if ((os.freemem()-(options.memJob*1048576*jobs)) > (options.minMem*1048576)){
									console.log("connecting...");
									s_wk_client = zmq.socket('pull');
									s_wk_client.on('message', on_message);
									s_wk_client.connect(options.uri);
								}
								break;
							case zmq.STATE_READY:
								if ((os.freemem()-(options.memJob*1048576*jobs)) < (options.minMem*1048576)){
									console.log("disconnecting...");
									s_wk_client.close();
									s_wk_client = {"_zmq":{"state" : zmq.STATE_CLOSED }};
								}
								break;
						}
					}
					updateStateRunning = false;
				};
				//initial setup
				s_wk_client.on('message', on_message);
				s_wk_client.connect(options.uri);
				setInterval(updateState,1000);
				r.listening = true;
			},

			"monitorJob" : function(jobId){

			}
			
		};

	/*==========  Helpers  ==========*/
	
	var guid = (function() {
		function s4() {
			return Math.floor((1 + Math.random()) * 0x10000)
				.toString(16)
				.substring(1);
		}
		return function() {
			return s4() + s4() + '-' + s4() + '-' + s4() + '-' +
				s4() + '-' + s4() + s4() + s4();
		};
	})();
	return r;
};


module.exports = main;
