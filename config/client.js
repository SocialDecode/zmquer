var server = require('../lib/server')(),
	fs = require('fs'),
	defaults = require("../config/defaults-client.json");
fs.exists("../config/client.json", function(exists) {
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
	server.listenJobs({
		'uri' : 'tcp://'+config.servername+':'+config.serverport,
		'uriret' : 'tcp://'+config.servername+':'+(config.serverport+1),
		'basepath' : config.basepath,
		'max' : config.max,/*Mex Number of simultaneous jobs*/
		'minMem' : config.minMem/*Minmum free memory to process jobs*/
	});
	console.log("Waiting for Jobs ...", config);
});

