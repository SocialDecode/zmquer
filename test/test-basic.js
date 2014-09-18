

var server = require('../lib/server')();
var jobTest = {
	"exec" : "ls",
	"args" : {
		"-A" : ""
	}
};

setTimeout(process.exit,3000); //daemon exit ...

module.exports = {
    setUp: function (callback) {
    	console.log("setUp");
		if (!server.ready) {
			server.initServer(require("../config/defaults-server.json"),function(){
				callback();
			});
		} else {
			callback();
			 
		}
		if (!server.listening){
			server.listenJobs({
				'uri' : 'tcp://127.0.0.1:3000',
				'uriret' : 'tcp://127.0.0.1:3001'
			});
		}
    },
    tearDown: function (callback) {
        // clean up
        callback();
    },
	JobAdd: function (test) {
		test.expect(1);
		server.addJob(jobTest,function(job){
			test.equal(job.exec,jobTest.exec);
			jobTest._id = job._id;
			setTimeout(test.done,2000); //fail safe to next test
		});
		
	},
	JobEXec : function(test){
		//todo: we should expect that the job no longer exists (meaning it executed withouth any issues)
		test.expect(1);
    test.ok(true, "this assertion should pass");
    test.done();
	}
};