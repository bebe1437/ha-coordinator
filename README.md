# ha-coordinator
ha-coordinator is a simple failover cluster and based on Zookeeper and build on Curator. 

## Build
./build.sh

## config
### file: conf/default.properties

zkHost= Zookeeper's Host.

timeout= Zookeeper's sessiontimeout.

cluster.name= Your HA Cluster's name.

agent.name= Your application node's name.

agent.retries= The maximun retries for register agent or process.

process.command= The script or command you want to run.

processors.maximum= The maximun processors you want to run in your cluster.

## Run
./start.sh {default.properties}
