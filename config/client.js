require('coffee-script/register');
var server = require('../lib/server.coffee')(),
	fs = require('fs'),
	defaults = require("../config/defaults-client.json");
fs.exists("../config/client.json", function(exists) {
	var config = {};
	if (exists) {
		config = require("../config/client.json");
		//extend with defaults
		for (var option in defaults){
			if (!config[option]) config[option] = defaults[option];
		}
	}else{
		config = defaults;
	}
	config.uri = 'tcp://'+config.servername+':'+config.serverport;
	config.uriret = 'tcp://'+config.servername+':'+(config.serverport+1);
	server.listenJobs(config);
	console.log("Waiting for Jobs ...", config);
});

