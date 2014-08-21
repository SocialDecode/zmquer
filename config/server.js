var server = require('../lib/server')(),
	fs = require('fs'),
	config = require("../config/defaults.json");
fs.exists("../config/server.json", function(exists) {
	if (exists) config = require("../config/server.json");
});
server.initServer(config,function(){
	console.log("Server is Up and Running !");
});