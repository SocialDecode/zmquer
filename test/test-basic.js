

var server = require('../lib/server')();
var jobTest = {
	"exec" : "ls",
	"args" : {
		"-A" : ""
	}
};


module.exports = {
    setUp: function (callback) {
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
			test.done();
		});
		setTimeout(test.done,5000); //fail safe
	},
	JobEXec : function(test){
		test.expect(1);
	}
};