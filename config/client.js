var server = require('../lib/server')(),
	fs = require('fs'),
	config = require("../config/defaults-client.json");
fs.exists("../config/client.json", function(exists) {
	if (exists) config = require("../config/client.json");
	server.listenJobs({
		'uri' : 'tcp://'+config.servername+':'+config.serverport,
		'uriret' : 'tcp://'+config.servername+':'+(config.serverport+1),
		'basepath' : config.basepath
	});
	console.log("Waiting for Jobs ...", config);
});

