

/**
*
* The Server
*
**/

var main = function(){

	/********************************
	*          HTTP Server          *
	********************************/
	var http   = require('http'),
		url      = require('url'),
		auth     = require("http-auth"),
		port     = 1337,
		response = {},
		sections = {},
		routes   = {GET:{},POST:{},PUT:{},DELETE:{}},
		config   = {},
		basic    = false;

	var allServer = function(req,res){
		if(url.parse(req.url).href=='/favicon.ico') return;
		path = url.parse(req.url,true).pathname;
		if(path.slice(-1)=='/') path = path.slice(0,-1);
		var outPut = function(req,res){
			res.writeHead(response.status, {"Content-Type": "application/json; charset=utf-8"});
			res.end(JSON.stringify(response.message));
		};
		var activeRoute = false;
		var possibleRoutes = [];
		var pathArr = path.split('/');
		var scopedRoutes = routes[req.method] || 'GET';

		for(var route in scopedRoutes){
			var routeArr = route.split('/');
			var valid = true;
			for(var i in routeArr){
				if(pathArr[i]==routeArr[i] || routeArr[i].slice(0,1)==':'){
					continue;
				}else{
					valid = false;
					break;
				}
			}
			if(valid) possibleRoutes.push(route);
		}
		for(var k in possibleRoutes){
			var possibleRoute = possibleRoutes[k];
			var possibleArr = possibleRoute.split('/');
			if(possibleArr.length==path.split('/').length){
				activeRoute = possibleRoute;
			}else if(possibleArr[possibleArr.length-1].slice(-1)=='?' && possibleArr.length-1==path.split('/').length){
				activeRoute = possibleRoute;
			}
		}
		var dispatcher = function(req,res,callback){
			response = {
				status : 404,
				message : '('+path+'): Page not found'
			};

			if(activeRoute === false){
				outPut(req,res);
				return;
			}

			var activeArr = activeRoute.split('/');
			req.args = {};
			for(var j in activeArr){
				var activePart = activeArr[j];
				if(activePart.slice(0,1)==':'){
					req.args[activePart.replace(/[\:\?]/g,'')] = path.split('/')[j];
				}
			}
			var body = '';
			req.on('data',function(data){
				body += data.toString();
			});
			req.on('end',function(){
				req.body = (body!=='') ? JSON.parse(body) : {};
				callback(req,res,function(){
					outPut(req,res);
				});
			});
		}(req,res,scopedRoutes[activeRoute]);
	};

	sections.getJobs = function(req,res,callback){
		var query = {};
		if(req.args.id){
			query = {_id:req.args.id};
		}
		col.find(query,{_id:1,created:1,exec:1,status:1,error:1}).toArray(function(err,docs){
			response.status = 200;
			response.message = docs;
			callback();
		});
	};

	sections.resetJobs = function(req,res,callback){
		var query = {
			_id:{
				$in:req.body.ids
			}
		};
		col.update(query,{
			$unset:{
				status: "",
				error : ""
			}
		},{
			multi : true,
			upsert : false
		},function(err,doc){
			response.status = 200;
			response.message = 'ok';
			callback();
		});
	};

	sections.deleteJobs = function(req,res,callback){
		var query = {
			_id:{
				$in:req.body.ids
			}
		};
		col.remove(query,function(err,coldel){
			response.status = 200;
			response.message = 'ok';
			callback();
		});
	};

	sections.logout = function(req,res,callback){
		response.status = 401;
		response.message = "You have been logged out";
		callback();
	};
	
	routes.PUT['/jobs/reset'] = sections.resetJobs;
	routes.GET['/jobs/:id?'] = sections.getJobs;
	routes.DELETE['/jobs'] = sections.deleteJobs;
	routes.GET['/logout'] = sections.logout;

	/*      End of HTTP Server     */


	var col = {},

		//Server side requirements
		os       = require("os"),
		mcli     = require('mongodb').MongoClient,
		ObjectID = require('mongodb').ObjectID,
		zmq      = require('zmq'),

		s_wk = zmq.socket('push'), //socket Work Que

		s_wc = zmq.socket('push'), //socket Work CallBack
		s_wc_client = zmq.socket('pull'), //socket Work CallBack Client

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

				/*==========  Setup DB Connection  ==========*/
				
				mcli.connect(config.mongouri, {server:{auto_reconnect: true}}, function(err, db){
					if(err) throw err;
					col = db.collection(config.coleccion);
					r.ready = true;
					if (config.onReady) config.onReady();


					var userfile = __dirname + "/../config/"+config.httpServer.authFile+".htpasswd";
					require('fs').exists(userfile, function(authexists) {
						if (!authexists) userfile += ".sample";
						basic = auth.basic({
							realm : "Commander Area.",
							file  : userfile
						});
						console.log("\n### --- HTTPServer online --- ###");
						console.log("     Listening on port: "+config.httpServer.port+"\n");
						http.createServer(basic,allServer).listen(config.httpServer.port);
					});
					

					

					syncDB();
					//setting up the work check Interval
					r.DBSyncInterval = setInterval(syncDB,5000);
				});

				/*==========  ZmQ listening  ==========*/
				
				//Setup zmq listening to push of commands (from clients)
				s_wk.bindSync('tcp://*:'+config.port);
				//Setup zmq for return data on the next port (from clients)
				s_wc_client.bindSync('tcp://*:'+(config.port+1));

				s_wc_client.on('message', function(msg){
					var c = JSON.parse(msg);
					if (c.id){
						var id = c.id;
						delete c.id;
						var idObj = id;
						try{
							idObj = new ObjectID(id);
						}catch(e){
						}
						var doUpdate = function(){
							col.update({_id:{ $in : [id,idObj] }},{$set: c},function(uperr,upcount){if (uperr || !upcount) console.log("Unable to find Job in DB");});
						};
						if (config.dumpSuccess && c.status == 3 && !c.err){
							col.remove({_id:{ $in : [id,idObj] }}, function(errdel,coldel){
								if (errdel || coldel<1){
									console.log("Unable to delete success object...");
									doUpdate();
								}
							});
						}else{
							doUpdate();
						}
						
					}else{
						if (c.startup){
							console.log("Re-submiting all Jobs assigned to",c.startup);
							//let's reset all status messages
							col.update(
								{
									'node.name' : c.startup,
									'status' : 2
								},{
									$unset: {
										status: ""
									}
								},
								{
									multi: true
								}, function(errdel,coldel){
									if (errdel){
										console.log("Unable to re-submit jobs ...");
									}else{
										console.log("Re-submited",coldel,"jobs");
									}
								});
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
				var syncDB = function(){
					enoughFreemem(config.minMem,function(freemem){
						if (!r.ready || s_wk._zmq.pending > config.maxQueSize*0.5 || !freemem) return;
						//Get the jobs in the que
						var jobs = {};
						for (var i=0;i<s_wk._zmq.pending;i++){
							jobs[(JSON.parse(s_wk._outgoing[i].toString('utf8').replace(/\,[0-9]+$/, ""))._id)] = true;
						}
						//Jobs with no status (to be picked up) or jobs that already should be qued
						col.find({
								$or : [
									{status : {$exists: false}},
									{status : {$lt:2}}
								]
						}).limit(config.maxQueSize).toArray(
							function(err,data){
								if (err) {console.log("error while getting DB data");return;}
								for (var i=0;i<data.length;i++){
									if (!jobs[data[i]._id]){
										//not in pile already !
										s_wk.send(JSON.stringify(data[i]));
										//updating mongodb
										col.update({_id:data[i]._id},{$set: {status:1,updated : ((new Date()).getTime())/ 1000 | 0}}, function(){});
									}
								}
							});
					});
					
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
				updateState = function(){
					if (s_wk_client._zmq.state == zmq.STATE_BUSY) return;
					enoughFreemem(options.minMem,function(freemem){
						//console.log(s_wk_client._zmq.state,jobs,options.max,freemem);
						switch (s_wk_client._zmq.state){
							case zmq.STATE_CLOSED:
								if (jobs < options.max  && freemem){
									console.log("connecting...");
									s_wk_client = zmq.socket('pull');
									s_wk_client.on('message', on_message);
									s_wk_client.connect(options.uri);
								}
								break;
							case zmq.STATE_READY:
								if (jobs >= options.max  || !freemem){
									console.log("disconnecting...");
									s_wk_client.close();
								}
								break;
						}
					})
					
				};
				

				var on_message = function(msg){
					//one more job
					jobs++;
					updateState();
					var c = JSON.parse(msg),
						args = "";
					console.log("got job ",c._id, "currently running",jobs);
					//sent back teh status job
					s_wc.send(JSON.stringify({
						id : c._id,
						status : 2,
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
					require('child_process').exec(cmd, {
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
							stdout : stdout,
							error : error,
							status : 3,
							file:file_ret
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

	var enoughFreemem = function(limit,done){
		var spawn = require('child_process').spawn;
		var prc = spawn('free',  []);

		prc.stdout.setEncoding('utf8');
		prc.stdout.on('data', function (data) {
			var str = data.toString();
			var lines = str.split(/\n/g);
			for(var i = 0; i < lines.length; i++) {
				lines[i] = lines[i].split(/\s+/);
			}

			var realfree = parseInt(lines[2][3],10);

			done(100-(realfree/os.totalmem()*100) > limit);
		});
		prc.on('error', function (code) {
			//fallback for other systems
			done(100-(os.freemem()/os.totalmem()*100) > limit);
		});
	};

	return r;

	
};


module.exports = main;


/**
*
* Utils
*
**/
