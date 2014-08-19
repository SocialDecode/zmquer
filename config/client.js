var server = require('../lib/server')();
var servername = process.argv[2] || "localhost";
var serverport = parseInt(process.argv[3] || 3000,10);
server.listenJobs({
	'uri' : 'tcp://'+servername+':'+serverport,
	'uriret' : 'tcp://'+servername+':'+(serverport+1)
});
console.log("Waiting for Jobs");