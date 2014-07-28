

var server;

exports.serverIsFunction = function(test){
    test.expect(1);
	var server = require('../lib/server')({
		port:8000,
		mongouri : "mongodb://localhost:27017/socialdecodefront"
	});
    test.ok(true, typeof server == "function" );
    test.done();
    console.log("ok");
};