var express = require('express');
var app = require('express')();
var server = require('http').Server(app);
var io = require('socket.io')(server);
var zk = require('./zk.js');
var cluster = process.argv.slice(2).join("");
console.log("HA cluster:"+cluster);

server.listen(5000);
zk.start(cluster);

app.use('/css', express.static('css'));
app.get('/', function (req, res) {
  res.sendfile(__dirname + '/index.html');
});

io.on('connection', function (socket) {
	zk.master(function(master){
		socket.emit('cluster',{cluster: cluster, master: master});
	});
	zk.task(function(agents){
		socket.emit('znode:agents', agents);
	});
	// zk.agents(function(nodes){
	// 	socket.emit('znode:agent', { agent: nodes });
	// });
	// zk.processes(function(nodes){
	// 	socket.emit('znode:process', { process: nodes });
	// });
	zk.conf(function(data){
		var obj = JSON.parse(data);
		socket.emit('znode:conf', { conf: obj} );
	});
  //socket.emit('news', { hello: 'world' });
  socket.on('my other event', function (data) {
    console.log(data);
  });
});