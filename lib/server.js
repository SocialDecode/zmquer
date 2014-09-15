

/**
*
* The Server
*
**/

var main = function(){
	var col = {},
		os = require("os"),
		mcli = require('mongodb').MongoClient,
		ObjectID = require('mongodb').ObjectID,
		zmq = require('zmq'),

		s_wk = zmq.socket('push'), //socket Work Que
		s_wk_client = zmq.socket('pull'), //socket Work Que Client

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
				var config = {},onReady;
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
				
				mcli.connect(config.mongouri, function(err, db){
					if(err) throw err;
					col = db.collection(config.coleccion);
					r.ready = true;
					if (config.onReady) config.onReady();

					r.syncDB();
					//setting up the work check Interval
					r.DBSyncInterval = setInterval(r.syncDB,5000);
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
						var doUpdate = function(){
							col.update({_id:{ $in : [id,new ObjectID(id)] }},{$set: c},function(uperr,upcount){if (uperr || !upcount) console.log("Unable to find Job in DB")});
						};
						if (config.dumpSuccess && c.status == 3 && !c.err){
							col.remove({_id:{ $in : [id,new ObjectID(id)] }}, function(errdel,coldel){
								if (errdel || coldel<1){
									console.log("Unable to delete success object...");
									doUpdate();
								}
							});
						}else{
							doUpdate();
						}
						
					}else{
						console.log("Not sure what to do with ",c);
					}
				});

				

				return r;
			},

			/*==========  DB Syncing  ==========*/
			
			/**
			*
			* Functions that Monitors the DB for Jobs and adds them (or remove's them from the que)
			*
			**/

			"syncDB" : function(){
				if (!r.ready) return;
				//Jobs with no status (to be picked up)
				col.find({status : {$exists: false}}).toArray(function(err,data){
					if (err) {console.log("error while getting DB data");return;}
					for (var i=0;i<data.length;i++){
						s_wk.send(JSON.stringify(data[i]));
						//updating mongodb
						col.update({_id:data[i]._id},{$set: {status:1,updated : ((new Date()).getTime())/ 1000 | 0}}, function(){});
					}

				});

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
				//setup the pulling of job
				s_wk_client.connect(options.uri);
				r.listening = true;
				//setup the work returns
				s_wc.connect(options.uriret);
				s_wk_client.on('message', function(msg){
					var c = JSON.parse(msg),
						args = "";
					console.log("got job ",c._id);
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
						if (c.env) fs.unlinkSync(filePath); //erasing input file
						stdout && console.log(stdout);
						//check if there is a file output
						var file_ret = (fs.existsSync(process.env.TMPDIR+c._id+"_out.json")) ? fs.readFileSync(process.env.TMPDIR+c._id+"_out.json", "utf8") : null;
						s_wc.send(JSON.stringify({
							id : c._id,
							stdout : stdout,
							error : error,
							status : 3,
							file:file_ret
						}));
					});

				});
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


/**
*
* Utils
*
**/
