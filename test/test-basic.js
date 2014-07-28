var server = require('../lib/server')({
	port:8000,
	mongouri : "mongodb://localhost:27017/socialdecodefront"
});

exports.serverIsFunction = function(test){
    test.expect(1);
    test.ok(true, typeof server == "function" );
    test.done();
};