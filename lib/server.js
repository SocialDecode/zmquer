var main = function(config){
	console.log(config);
};

if (!module.parent) {
    main({
    	port : 8000
    });
} else {
	exports = module.exports = main;
}