

/**
*
* The Server
*
**/

var main = function(){
	var col = {},
		jobsCounter = [0],
		httpserver = require('./httpserver'),
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

		s_wk = zmq.socket('push',{_maxListeners:20}), //socket Work Que

		s_wc = zmq.socket('push'), //socket Work CallBack
		s_wc_client = zmq.socket('pull',{_maxListeners:20}), //socket Work CallBack Client

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
					//Setup zmq listening to push of commands (from clients)
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
								cfg        : config,
								processing : couch_processing,
								universe   : jobsCounter
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
									cfg        : config,
									processing : couch_processing,
									universe   : jobsCounter
								});
								
							}
						});
					break;
				}

				/*==========  ZmQ listening  ==========*/
								
				s_wc_client.on('message', function(msg){
					var c = JSON.parse(msg);
					if (c.id){
						var id = (c.idtype == "string" ? c.id : ObjectID(c.id));
						delete c.id;
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
										couchque.insert({"_deleted" : true,"_rev" : c._rev},id, function(errup,res){
											if (errup){
												if (errup.status_code === 409){
													console.log(":|",c.node?c.node.name:"?",id,"Job Already completed",errup.description);
												}else{
													console.log(":|",c.node?c.node.name:"?",id,errup.description);
												}
											}else{
												console.log(":)",c.node?c.node.name:"?",id);
											}
											delete couch_processing[id];
											delete couch_errors[id];
											
									});
								break;
							}
							
						}else{
							doUpdate();
						}
						
					}else{
						if (c.startup){
							console.log("Server Reconncted ... ",c.startup);
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
				var couch_processing = {},
					couch_errors = {};
				var syncDB = function(){
					if (!r.ready  || os.freemem() < (config.minMem*1048576)) {process.nextTick(syncDB);return;}
					//Jobs with no status (to be picked up) or jobs that already should be qued
					switch (dbtype){
						case "mongo":
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
							couchque.list({},function(geterr,getbody){
								if (!geterr){
									//control pruning
									var cpsize = Object.keys(couch_processing).length;

									for (var jobId in couch_processing){
										var found = false;
										jobsCounter[0] = getbody.rows.length;
										for (var y=0;y<getbody.rows.length;y++){
											if (getbody.rows[y].id == jobId){found=true;break;}
										}
										if (!found) delete couch_processing[jobId];
									}
									var couch_toprocess = [];
									for (var x=0;x<getbody.rows.length;x++){
										if (!couch_processing[getbody.rows[x].id]) couch_toprocess.push([getbody.rows[x].id,getbody.rows[x].value.rev]);
									}
									if (couch_toprocess.length<1){process.nextTick(syncDB);return;}
									couch_toprocess = couch_toprocess.slice(0,config.dbIncremental);
									process.stdout.write("["+s_wk._zmq.pending+"."+cpsize+"]q?"+couch_toprocess.length);
									var keys = [], revs = {};
									for (var y=0;y<couch_toprocess.length;y++) {keys.push(couch_toprocess[y][0]);revs[couch_toprocess[y][0]] = couch_toprocess[y][1];}
									couchque.list({
										keys:keys,
										include_docs : true
									},function(bulkerr,bulkbody){
										if (bulkerr) {console.log("error while getting DB data",bulkerr);process.nextTick(syncDB);return;}
										for (var value in justSent){if (justSent[value]>tickreset) {delete justSent[value];}} //already timedout
										for (var i=0;i<bulkbody.rows.length;i++){
											if (!bulkbody.rows[i].doc) continue;
											var id = bulkbody.rows[i].doc._id;
											couch_processing[id] = 1;
											bulkbody.rows[i].doc["_rev"] = revs[id];
											bulkbody.rows[i].doc["idtype"] = "string";
											s_wk.send(config.jsonpack ? jsonpack.pack(bulkbody.rows[i].doc) : JSON.stringify(bulkbody.rows[i].doc));
											if (justSent[id]){justSent[id]++;}else{justSent[id] = 1;} //increment the waiting on one ir set it as one
										}
										console.log("_");

										//Reset jobs when the queue is 0
										if(s_wk._zmq.pending===0){
											for (var jobId in couch_processing){
												if(couch_processing[jobId]===1 && !justSent[jobId]) delete couch_processing[jobId];
											}
										}

										process.nextTick(syncDB);return;
									});
								}else{
									console.log("error while getting DB data");process.nextTick(syncDB);return;
								}
							});
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

				//timed restart of job processing
				var updateState = function(){
					if (s_wk_client._zmq.state == zmq.STATE_BUSY) return;
					//console.log(s_wk_client._zmq.state,jobs,options.max,freemem);
					switch (s_wk_client._zmq.state){
						case zmq.STATE_CLOSED:
							if (os.freemem() > (options.minMem*1048576)){
								console.log("connecting...");
								s_wk_client = zmq.socket('pull');
								s_wk_client.on('message', on_message);
								s_wk_client.connect(options.uri);
							}
							break;
						case zmq.STATE_READY:
							if (os.freemem() < (options.minMem*1048576)){
								console.log("disconnecting...");
								s_wk_client.close();
							}
							break;
					}
				};
				var on_message = function(msg){
					//one more job
					jobs++;
					updateState();
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
								totamem : os.totalmem()
							},
							file:file_ret,
							_rev : c._rev
						}));
						//one less job
						jobs--;
						updateState();
						console.log("job done",c._id, "currently running",jobs);
					});
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
