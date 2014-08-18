
module.exports = function(grunt){
	grunt.initConfig({
		nodeunit: {
	    	all: ['test/**/*.js','lib/**/*.js']
	  },
	  watch: {
	  scripts: {
	    files: ['test/**/*.js','lib/**/*.js'],
	    tasks: ['clear','nodeunit'],
	    options: {
	      debounceDelay: 250,
	    },
	  },
	},
	});
	grunt.loadNpmTasks('grunt-clear');
	grunt.loadNpmTasks('grunt-contrib-nodeunit');
	grunt.loadNpmTasks('grunt-contrib-watch');
};

