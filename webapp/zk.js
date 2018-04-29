var util = require('util');
var zookeeper = require('node-zookeeper-client');

var CONNECTION_STRING = 'localhost:2181';
var CLUSTER = 'ha-test'
var AGENT_NODE = util.format('/%s/agents', CLUSTER);
var PROCESS_NODE = util.format('/%s/processes', CLUSTER);
var CONF_NODE = util.format('/%s/conf', CLUSTER);
var MASTER_NODE= util.format('/%s/info/master', CLUSTER);
var TASK_NODE= util.format('/%/info/task', CLUSTER);
var client = zookeeper.createClient(CONNECTION_STRING);

exports.start = function(cluster, fn){
	AGENT_NODE = util.format('/%s/agents', cluster);
	PROCESS_NODE = util.format('/%s/processes', cluster);
	CONF_NODE = util.format('/%s/conf', cluster);
	MASTER_NODE= util.format('/%s/info/master', cluster);
	TASK_NODE= util.format('/%s/info/task', cluster);
	client.connect();
}

exports.agents = function(fn){
	listChildren(client, AGENT_NODE, fn);
}

exports.processes = function(fn){
	listChildren(client, PROCESS_NODE, fn);
}

exports.conf = function(fn){
	retrieveData(client, CONF_NODE, fn);
}

exports.master = function(fn){
	listChildren(client, MASTER_NODE, fn);
}

exports.task = function(fn){
	retrieveData(client, TASK_NODE, function(data){
		if(data!=undefined){
			var obj = JSON.parse(data);
			var list = obj.pl;
			var agents = [];

			listChildren(client, AGENT_NODE, function(nodes){
				nodes.forEach(function(node){
					const chosen = list.indexOf(node)>-1;
					agents.push({agent: node, chosen: chosen});
				});

				listChildren(client, PROCESS_NODE, function(nodes){
					agents.forEach(function(data){
						const running = nodes.indexOf(data.agent)>-1;
						data.running = running;
					});

					fn(agents);
				});
			});
		}
	});
}

function listChildren(client, path, fn) {
    client.getChildren(
        path,
        function (event) {
            console.log('Got watcher event: %s', event);
            listChildren(client, path, fn);
        },
        function (error, children, stat) {
            if (error) {
                console.log(
                    'Failed to list children of %s due to: %s.',
                    path,
                    error
                );
                return;
            }

            console.log('Children of %s are: %j.', path, children);
            fn(children);
        }
    );
}

function retrieveData(client, path, fn) {
    client.getData(
        path,
        function (event) {
            console.log('Got watcher event: %s', event);
            retrieveData(client, path, fn);
        },
        function (error, data, stat) {
            if (error) {
                console.log(
                    'Failed to getData from %s due to: %s.',
                    path,
                    error
                );
                return;
            }

            console.log(
                'Node: %s has data: %s, version: %d',
                path,
                data ? data.toString() : undefined,
                stat.version
            );
            fn(data? data.toString(): undefined);
        }
    );
}