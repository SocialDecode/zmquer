/********************************
*          HTTP Server          *
********************************/
var http   = require('http'),
  url      = require('url'),
  auth     = require('http-auth'),
  fs       = require('fs'),
  sections = {},
  routes   = {GET:{},POST:{},PUT:{},DELETE:{}},
  couch_processing = {},
  parentCallback   = {},
  staticsFolder    = '../statics/';

var allServer = function(req,res){
  if(url.parse(req.url).href=='/favicon.ico') return;
  path = url.parse(req.url,true).pathname;
  if(path.slice(-1)=='/') path = path.slice(0,-1);
  res.send = function(info){
    var cType = {"Content-Type": "text/html; charset=utf-8"};
    var status = 404;
    var message = '';
    switch(typeof info){
      case 'object':
        status = 200;
        message = JSON.stringify(info);
      break;
      case 'number':
        status = info;
      break;
      case 'string':
        status = 200;
        message = info;
    }
    res.writeHead(status, cType);
    res.end(message);
  };
  res.json = function(info){
    var cType = {"Content-Type": "application/json; charset=utf-8"};
    var status = 404;
    var message = '';
    switch(typeof info){
      case 'object':
        if(Array.isArray(info)){
          status = info[0];
          message = info[1];
        }else{
          status = 200;
          message = info;
        }
      break;
      case 'number':
        status = info;
      break;
      case 'string':
        status = 200;
        message = info;
    }
    res.writeHead(status, cType);
    res.end(JSON.stringify(message));
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
    if(activeRoute === false){
      res.json([404,'('+path+'): Page not found']);
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
      req.body = (body!=='') ? require("querystring").parse(body) : {};
      callback(req,res);
    });
  }(req,res,scopedRoutes[activeRoute]);
};

sections.logout = function(req,res){
  console.log('\nByebye:',req.connection.remoteAddress,'\n');
  res.json([401,'You have been logged out!']);
};

_getStatus = function(){
  var snapshot = {
    processing : JSON.parse(JSON.stringify(processing)),
    universe : universe[0],
    errors : errors[0]
  };

  var executing = 0;
  var total = snapshot.universe;
  var pending = 0;
  var completed = 0;

  for(var i in snapshot.processing){
    var itm = snapshot.processing[i];
    if(itm==3) completed++;
    if(itm==2) executing++;
  }
  pending = total - (executing-completed);
  var retval =  {total:total,pending:pending,executing:executing,withErr:snapshot.errors,servers:servers,completed:completed};
  return retval;
};

sections.index = function(req,res){
  console.log('\nGot user login from:',req.connection.remoteAddress,'\n');
  fs.readFile(staticsFolder+'index.html',{encoding:'utf-8'},function(err,contents){
    res.send(contents);
  });
};

sections.status = function(req,res){
  res.json(_getStatus());
};

sections.resetJobs = function(req,res){
  console.log('\nResetting Error Jobs\n');
  parentCallback.resetJobs();
  res.json(_getStatus());
};

sections.resetServer = function(req,res){
  console.log("\nResetting Server :",req.body.cual);
  res.send("");
};

routes.GET['/logout'] = sections.logout;
routes.GET['/status'] = sections.status;
routes.GET[''] = sections.index;
routes.POST['/resetJobs'] = sections.resetJobs;
routes.POST['/resetServer'] = sections.resetServer;

/*      End of HTTP Server     */


exports.syncData = function(){
  parentCallback.syncData(function(data){
    processing = data.processing;
    universe   = data.universe;
    servers   = data.servers;
    errors   = data.errors;
  });
};

exports.startHttp = function(data){
  config     = data.cfg;
  parentCallback = data.callback;
  exports.syncData();
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