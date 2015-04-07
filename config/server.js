require('coffee-script/register');
var server = require('../lib/server.coffee')(),
	fs = require('fs'),
	defaults = require(__dirname+"/defaults-server.json");
	fs.exists(__dirname+"/server.json", function(exists) {
		var config = {};
		if (exists) {
			config = require(__dirname+"/server.json");
			//extend with defaults
			for (var option in defaults){
				if (typeof config[option] == "undefined") config[option] = defaults[option];
			}
		}else{
			config = defaults;
		}
		
		server.initServer(config,function(){
			console.log("\n### --- ZMQ Server online --- ###");
			console.log("     Listening on port: "+config.port);
		});
});
