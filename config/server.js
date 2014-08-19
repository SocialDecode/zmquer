var server = require('../lib/server')();
server.initServer(require("../config/defaults.json"),function(){
	console.log("Server is Up and Running !");
});