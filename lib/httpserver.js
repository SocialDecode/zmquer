/********************************
*          HTTP Server          *
********************************/
var http   = require('http'),
  url      = require('url'),
  auth     = require("http-auth"),
  response = {},
  sections = {},
  routes   = {GET:{},POST:{},PUT:{},DELETE:{}},
  couch_processing = {};

var allServer = function(req,res){
  if(url.parse(req.url).href=='/favicon.ico') return;
  path = url.parse(req.url,true).pathname;
  if(path.slice(-1)=='/') path = path.slice(0,-1);
  var outPut = function(req,res){
    res.writeHead(response.status, {"Content-Type": "application/json; charset=utf-8"});
    res.end(JSON.stringify(response.message));
  };
  var activeRoute = false;
  var possibleRoutes = [];
  var pathArr = path.split('/');
  var scopedRoutes = routes[req.method] || 'GET';

  for(var route in scopedRoutes){
    var routeArr = route.split('/');
    var valid = true;
    for(var i in routeArr){
      if(pathArr[i]==routeArr[i] || routeArr[i].slice(0,1)==':'){
        continue;
      }else{
        valid = false;
        break;
      }
    }
    if(valid) possibleRoutes.push(route);
  }
  for(var k in possibleRoutes){
    var possibleRoute = possibleRoutes[k];
    var possibleArr = possibleRoute.split('/');
    if(possibleArr.length==path.split('/').length){
      activeRoute = possibleRoute;
    }else if(possibleArr[possibleArr.length-1].slice(-1)=='?' && possibleArr.length-1==path.split('/').length){
      activeRoute = possibleRoute;
    }
  }
  var dispatcher = function(req,res,callback){
    response = {
      status : 404,
      message : '('+path+'): Page not found'
    };

    if(activeRoute === false){
      outPut(req,res);
      return;
    }

    var activeArr = activeRoute.split('/');
    req.args = {};
    for(var j in activeArr){
      var activePart = activeArr[j];
      if(activePart.slice(0,1)==':'){
        req.args[activePart.replace(/[\:\?]/g,'')] = path.split('/')[j];
      }
    }
    var body = '';
    req.on('data',function(data){
      body += data.toString();
    });
    req.on('end',function(){
      req.body = (body!=='') ? JSON.parse(body) : {};
      callback(req,res,function(){
        outPut(req,res);
      });
    });
  }(req,res,scopedRoutes[activeRoute]);
};

sections.logout = function(req,res,callback){
  response.status = 401;
  response.message = "You have been logged out";
  callback();
};

sections.status = function(req,res,callback){
  response.status = 200;
  var snapshot = {
    processing : JSON.parse(JSON.stringify(processing)),
    universe : universe[0]
  };

  var withErr = 0;
  var executing = 0;
  var total = snapshot.universe;
  var pending = 0;

  for(var i in snapshot.processing){
    var itm = snapshot.processing[i];
    if(itm==3) withErr++;
    if(itm==2) executing++;
  }

  pending = total - (withErr+executing);

  response.message = {total:total,pending:pending,executing:executing,withErr:withErr};
  callback();
};

routes.GET['/logout'] = sections.logout;
routes.GET['/status'] = sections.status;

/*      End of HTTP Server     */


exports.startHttp = function(data){

  //processing y universe son REFERENCIAS heredadas de server.js

  config     = data.cfg;
  processing = data.processing;
  universe   = data.universe;
  var userfile = __dirname + "/../config/"+config.httpServer.authFile+".htpasswd";
  require('fs').exists(userfile, function(authexists) {
    if (!authexists) userfile += ".sample";
    basic = auth.basic({
      realm : "Commander Area.",
      file  : userfile
    });
    console.log("\n### --- HTTPServer online --- ###");
    console.log("     Listening on port: "+config.httpServer.port+"\n");
    http.createServer(basic,allServer).listen(config.httpServer.port);
  });
};