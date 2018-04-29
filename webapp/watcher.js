var util = require('util');
var zookeeper = require('node-zookeeper-client');
class Watcher{
    static watch(event){
        console.log('Got watcher event: %s', event);
        retrieveData(client, path, fn);
    },
    static retrieve(){
        
    }
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