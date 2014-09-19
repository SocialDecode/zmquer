var server = require('../lib/server')(),
	fs = require('fs'),
	config = require("../config/defaults-server.json");
	fs.exists("../config/server.json", function(exists) {
		if (exists) config = require("../config/server.json");
		server.initServer(config,function(){
			console.log("\n### --- ZMQ Server online --- ###");
			console.log("     Listening on port: "+config.port);
		});
});
