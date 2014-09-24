var server = require('../lib/server')(),
	fs = require('fs'),
	defaults = require("../config/defaults-server.json");
	fs.exists("../config/server.json", function(exists) {
		var config = {};
		if (exists) {
			config = require("../config/server.json");
			//extend with defaults
			for (var option in defaults){
				if (!config[option]) config[option] = defaults[option];
			}
		}else{
			config = defaults;
		}
		
		server.initServer(config,function(){
			console.log("\n### --- ZMQ Server online --- ###");
			console.log("     Listening on port: "+config.port);
		});
});
