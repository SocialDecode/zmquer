

/**
*
* The Server
*
**/

var main = function(){
	var col = {},
		couch_processing = {},
		jobsCounter = [0],
		httpserver = require('./httpserver'),
		async = require("async"),
		hostJobs = {},
		httpCB = {
			syncData : function(cb){
				cb({
					processing:couch_processing,
					universe:jobsCounter,
					servers : hostJobs
				});
			},
			resetJobs : function(){
				cleanedProc = {};
				for(var kj in couch_processing){
					if(couch_processing[kj]!=3) cleanedProc[kj] = couch_processing[kj];
				}
				couch_processing = cleanedProc;
				cleanedProc = null;
				httpserver.syncData();
			}
		},
		nano     = require('nano'),
		jsonpack = require('jsonpack/main'),

		//Server side requirements
		os       = require("os"),
		mcli     = require('mongodb').MongoClient,
		couchdb  = null,
		couchque = null,
		tickreset = 5,
		justSent = {},

		ObjectID = require('mongodb').ObjectID,
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

				var dbtype = (config.couch && config.couch.server && config.couch.database) ? "couch" : "mongo";
				switch (dbtype){
					case "mongo":
						/*==========  Setup DB Connection [MONGO]  ==========*/
						mcli.connect(config.mongouri, {server:{auto_reconnect: true}}, function(err, db){
							if(err) throw err;
							col = db.collection(config.coleccion);
							r.ready = true;
							startClients();
							if (config.onReady) config.onReady();
							httpserver.startHttp({
								cfg      : config,
								callback : httpCB
							});
							//reset all jobs that supossedly are on the que
							col.update(
								{
									'status' : 1
								},{
									$unset: {
										status: ""
									}
								},
								{
									multi: true
								}, function(errdel,coldel){
									if (errdel){
										console.log("Unable to re-que jobs ...");
									}else{
										console.log("Re-qued",coldel,"jobs");
									}
								});
							syncDB();
						});
					break;
					case "couch":
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
								syncDB();
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
					break;
				}

				/*==========  ZmQ listening  ==========*/
				var couchDelCargo =  async.cargo(function (tasks, callback){
					console.log("droping",tasks.length,"job(s)");
					var docs = [];
					for (var i=0;i<tasks.length;i++){
						docs.push({
							"_id" : tasks[i].id,
							"_deleted" : true,
							"_rev" : tasks[i].c._rev
						});
					}
					couchque.bulk({docs:docs},function(err,resp){
						if (err){
							console.log("error while dropping..",err);
							couchDelCargo.push(tasks);
						}else{
							for (var x=0;x<resp.length;x++){
								if (!resp[x].error){
									console.log(":) ",resp[x].id);
								}else{
									console.log(":| ",resp[x].id,resp[x].error);
								}
								delete couch_processing[resp[x].id];
								delete couch_errors[resp[x].id];
							}
							callback();
						}
					});
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
							switch (dbtype){
								case "mongo":
									col.update({_id:id},{$set: c},function(uperr,upcount){
										if (uperr) {
											console.log("Error while updating job",id,uperr);
										}else{
											if (upcount<1) console.log("Unable to find job to update",id);
										}
									});
								break;
								case "couch":
									couch_processing[id] = c.status;
									if (c.status==3 && c.error){
										console.log(":(",c.node?c.node.name:"?",id,c.error.message);
										if (couch_errors[id]) {
											couch_errors[id]++;
										}else{
											couch_errors[id] = 1;
										}
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
								break;
							}
						};
						if (config.dumpSuccess && c.status == 3 && !c.error){
							switch (dbtype){
								case "mongo":
									col.remove({_id:id}, function(errdel,coldel){
										if (errdel) {
											console.log("Error while deleting job",id,uperr);
											doUpdate();
										}else{
											if (coldel<1){
												console.log("Unable to find job to delete",id);
												doUpdate();
											}
										}
									});
								break;
								case "couch":
									couch_processing[id] = 3;
									couchDelCargo.push({c:c,id:id});
								break;
							}
							
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
				if (!r.ready  || os.freemem() < (config.minMem*1048576)) {
					process.nextTick(function(){getKeysCargo.push(tasks);});
					callback();
				}else{
					for(var i=0; i<tasks.length; i++){
						var task = tasks[i];
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
										console.log("["+s_wk._zmq.pending+"."+Object.keys(couch_processing).length+"]q->"+couch_toprocess.length);
										getDocsCargo.push(couch_toprocess);
									}
									//run itself again
									if (getbody.rows.length<task.limit){
										process.nextTick(function(){getKeysCargo.push({limit:task.limit});});
									}else{
										process.nextTick(function(){
											getKeysCargo.push({limit:task.limit,startkey:getbody.rows[getbody.rows.length-1].id});
										});
									}
								}else{
									process.nextTick(function(){getKeysCargo.push({limit:task.limit});});
								}
							}
							callback();
						});
					}
				}
			},1);
			var getDocsCargo = async.cargo(function(couch_toprocess, callback){
				if (!r.ready  || os.freemem() < (config.minMem*1048576) || couchDelCargo.length()>couchDelCargo.payload) {
					process.nextTick(function(){
						getDocsCargo.push(couch_toprocess);
					});
					callback();
				}else{
					var keys = [], revs = {};
					for (var y=0;y<couch_toprocess.length;y++) {
						keys.push(couch_toprocess[y][0]);
						revs[couch_toprocess[y][0]] = couch_toprocess[y][1];
					}
					console.log("d"+keys.length+"?");
					couchque.list({
						keys:keys,
						include_docs : true
					},function(bulkerr,bulkbody){
						if (bulkerr) {
							console.log("error while getting DB data",bulkerr);
							process.nextTick(function(){
								getDocsCargo.push(couch_toprocess);
							});
							callback();
						}
						
						for (var value in justSent){if (justSent[value]>tickreset) {delete justSent[value];}justSent[value]++;} //already timedout

						for (var i=0;i<bulkbody.rows.length;i++){
							if (!bulkbody.rows[i].doc) continue;
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
				var couch_errors = {};
				var syncDB = function(){
					//Jobs with no status (to be picked up) or jobs that already should be qued
					switch (dbtype){
						case "mongo":
							if (!r.ready  || os.freemem() < (config.minMem*1048576)) {process.nextTick(syncDB);return;}
							col.find({$or:[{status : {$exists: false}},{status : null}]}).limit(config.dbIncremental).toArray(
								function(err,data){
									if (err) {console.log("error while getting DB data");process.nextTick(syncDB);return;}
									if (data.length<1){process.nextTick(syncDB);return;}
									process.stdout.write("q+"+data.length);
									var ids = [];
									for (var i=0;i<data.length;i++){
											//send to que
											data[i].idtype = typeof data[i];
											data[i]._id += "";
											s_wk.send(jsonpack.pack(data[i]));
											ids.push( data[i].idtype == "string" ? data[i]._id : new ObjectID(data[i]._id));
									}
									col.update({_id:{ $in : ids }},{$set: {status:1,updated : ((new Date()).getTime())/ 1000 | 0}},{multi:true}, function(errque,howque){
										if (errque) {
											console.log("Error while updating que jobs",ids,errque);
										}else{
											if (howque<1) console.log("Unable to find jobs to update",ids);
										}
										console.log("_");
										process.nextTick(syncDB);
									});
								}
							);
						break;
						case "couch":
							getKeysCargo.push({limit:500});
						break;
					}
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
				var jobs = 0,
					s_wk_client = zmq.socket('pull'); //socket Work Que Client;
				//setup the work returns
				s_wc.connect(options.uriret);
				s_wc.send(JSON.stringify({startup : os.hostname()}));

				var on_message = function(msg){
					//one more job
					jobs++;
					var c = options.jsonpack ? jsonpack.unpack(String(msg)) : JSON.parse(msg),
						args = "";
					console.log("got job ",c._id,"currently running",jobs);
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
					var cmd = (options.basepath ? options.basepath + "/" : "") + c.exec + args;
					var env = process.env;
					env.jobId = c._id;
					if (!env.TMPDIR) env.TMPDIR = "/var/tmp/";
					//create a file for the arguments in env
					var fs = require('fs'),
						filePath = env.TMPDIR + c._id +'.json';
					if (c.env) fs.writeFileSync( filePath, JSON.stringify(c.env), 'utf-8');
					var path = cmd.split("/");path.pop();path = path.join("/");
					env.PATH +=":"+path;
					require('child_process').exec(cmd/*+";"+__dirname+"/closer.js "+c._id*/, {
						cwd : path,
						env : env
					} ,function(error, stdout, stderr) {
						if (c.env && fs.existsSync(filePath)) fs.unlinkSync(filePath); //erasing input file
						stdout && console.log(stdout);
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
								jobs : String(jobs-1)
							},
							file:file_ret,
							_rev : c._rev
						}));
						//one less job
						jobs--;
						console.log("job done",c._id, "currently running",jobs);
					});
				};
				//timed restart of job processing
				var updateState = function(){
					if (s_wk_client._zmq.state !== zmq.STATE_BUSY){
						//console.log(s_wk_client._zmq.state,jobs,options.max,freemem);
						switch (s_wk_client._zmq.state){
							case zmq.STATE_CLOSED:
								if ((os.freemem()-(options.memJob*1048576*jobs)) > (options.minMem*1048576)){
									console.log("connecting...");
									s_wk_client = zmq.socket('pull');
									s_wk_client.on('message', on_message);
									var x = s_wk_client.connect(options.uri);
								}
								break;
							case zmq.STATE_READY:
								if ((os.freemem()-(options.memJob*1048576*jobs)) < (options.minMem*1048576)){
									console.log("disconnecting...");
									s_wk_client.close();
								}
								break;
						}
					}
					setImmediate(updateState);
				};
				//initial setup
				s_wk_client.on('message', on_message);
				s_wk_client.connect(options.uri);
				setImmediate(updateState);
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
