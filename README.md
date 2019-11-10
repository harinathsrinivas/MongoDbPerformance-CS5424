# Team13-M-MongoDB

## Introduction
MongoDB benchmarking - Evaluates how MongoDB performs for large number of transactions with different configuration options for various values of Number of clients (Number of transaction files executed).

## Installing instructions
### 1. Install MongoDB RHEL 7 (>=4.2.1) in temp folder and create required folders
```
mkdir /temp/newMongo/
cd /temp/newMongo/
wget https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-rhel70-4.2.1.tgz
gunzip mongodb-linux-x86_64-rhel70-4.2.1.tgz
tar xvf mongodb-linux-x86_64-rhel70-4.2.1.tar
mkdir -p /temp/newMongo/data/data1 /temp/newMongo/data/data2 /temp/newMongo/data/data3 /temp/newMongo/data/configsvr /temp/newMongo/data/mongos
mkdir -p /temp/newMongo/log/data1 /temp/newMongo/log/data2 /temp/newMongo/log/data3 /temp/newMongo/log/configsvr /temp/newMongo/log/mongos
```

### 2. Install Maven(>=3.3.9) in temp folder
```
cd /temp
wget http://download.nus.edu.sg/mirror/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
tar xzvf apache-maven-3.3.9-bin.tar.gz
```
### 3. SET ENVIRONMENT VARIABLES
```
export PATH=/temp/newMongo/mongodb-linux-x86_64-rhel70-4.2.1/bin:$PATH
```

## Running in Cluster with three nodes replicas for each shard, and three node configserver

### 1. Setup ConfigServers (Port#: 27020) on node 1, node 3, node 5
##### a. Run mongod for configsvr
```#### For each of node 1, node 3 and node 5 run the following with respective IP addresses:

# On node 1:
mongod --configsvr --dbpath ../../data/configsvr --port 27020 --replSet configsrvreplica --logpath ../../log/configsvr/monod.log --fork --bind_ip 192.168.56.159
# On node 3:
mongod --configsvr --dbpath ../../data/configsvr --port 27020 --replSet configsrvreplica --logpath ../../log/configsvr/monod.log --fork --bind_ip 192.168.56.161
# On node 5:
mongod --configsvr --dbpath ../../data/configsvr --port 27020 --replSet configsrvreplica --logpath ../../log/configsvr/monod.log --fork --bind_ip 192.168.56.163
```

#### Use one of the members to connect to mongo shell::
#### b. Connect to config server via mongo shell.
```
mongo --host 192.168.56.159 --port 27020
```
##### c. Initiate the config replica
```
rs.initiate(
  {
    _id: "configsrvreplica", 
    configsvr: true, 
    members: [ 
      { _id : 0, host : "192.168.56.159:27020" }, 
      { _id : 1, host : "192.168.56.161:27020" }, 
      { _id : 2, host : "192.168.56.163:27020" } 
    ]
  }
)
```
##### d. Check the status of connection. Three members must be there in the set.
```
rs.status()
```
### 2. Setup mongos (Port#: 27017) on all nodes
In all the 5 nodes, run the below command with ips of the respective machines.
```
mongos --configdb "configsrvreplica/192.168.56.159:27020,192.168.56.161:27020,192.168.56.163:27020" --fork --logpath ../../log/mongos/monos.log --bind_ip 192.168.56.159
```

### 3. Initialization of replica set (Port: 27021 – 27025) for each shard
##### a. For shard1 run the following in respective nodes
```
# From node 1:
mongod --shardsvr --replSet "shard1" --dbpath ../../data/data1 --fork --logpath ../../log/data1/mongod.log --port 27021 --bind_ip 192.168.56.159
# From node 2:
mongod --shardsvr --replSet "shard1" --dbpath ../../data/data1 --fork --logpath ../../log/data1/mongod.log --port 27021 --bind_ip 192.168.56.160
# From node 3:
mongod --shardsvr --replSet "shard1" --dbpath ../../data/data1 --fork --logpath ../../log/data1/mongod.log --port 27021 --bind_ip 192.168.56.161
```
Connect to any one of the running mongod instance, port 27021 and run the following.
```
mongo --host 192.168.56.159 --port 27021
```
and run the following code
```
rs.initiate(
  {
    _id : "shard1",
    members: [
      { _id : 0, host : "192.168.56.159:27021" },
      { _id : 1, host : "192.168.56.160:27021" },
      { _id : 2, host : "192.168.56.161:27021" },
    ]
  }
)
```
##### b. For shard2 run the following in respective nodes
```
# From node 2:
mongod --shardsvr --replSet "shard2" --dbpath ../../data/data2 --fork --logpath ../../log/data2/mongod.log --port 27022 --bind_ip 192.168.56.160
# From node 3:
mongod --shardsvr --replSet "shard2" --dbpath ../../data/data2 --fork --logpath ../../log/data2/mongod.log --port 27022 --bind_ip 192.168.56.161
# From node 4:
mongod --shardsvr --replSet "shard2" --dbpath ../../data/data1 --fork --logpath ../../log/data1/mongod.log --port 27022 --bind_ip 192.168.56.162
```
Connect to any one of the running mongod instance, port 27022 and run the following.
```
mongo --host 192.168.56.160 --port 27022
```
and run the following code
```
rs.initiate(
  {
    _id : "shard2",
    members: [
      { _id : 1, host : "192.168.56.160:27022" },
      { _id : 2, host : "192.168.56.161:27022" },
      { _id : 3, host : "192.168.56.162:27022" }
    ]
  }
)
```
##### c. For shard3 run the following in respective nodes
```
# From node 3:
mongod --shardsvr --replSet "shard3" --dbpath ../../data/data3 --fork --logpath ../../log/data3/mongod.log --port 27023 --bind_ip 192.168.56.161
# From node 4:
mongod --shardsvr --replSet "shard3" --dbpath ../../data/data2 --fork --logpath ../../log/data2/mongod.log --port 27023 --bind_ip 192.168.56.162
# From node 5:
mongod --shardsvr --replSet "shard3" --dbpath ../../data/data1 --fork --logpath ../../log/data1/mongod.log --port 27023 --bind_ip 192.168.56.163
```
Connect to any one of the running mongod instance, port 27023 and run the following.
```
mongo --host 192.168.56.161 --port 27023
```
and run the following code
```
rs.initiate(
  {
    _id : "shard3",
    members: [
      { _id : 2, host : "192.168.56.161:27023" },
      { _id : 3, host : "192.168.56.162:27023" },
      { _id : 4, host : "192.168.56.163:27023" }
    ]
  }
)
```
##### d. For shard4 run the following in respective nodes
```
# From node 1:
mongod --shardsvr --replSet "shard4" --dbpath ../../data/data2 --fork --logpath ../../log/data2/mongod.log --port 27024 --bind_ip 192.168.56.159
# From node 2:
mongod --shardsvr --replSet "shard4" --dbpath ../../data/data3 --fork --logpath ../../log/data3/mongod.log --port 27024 --bind_ip 192.168.56.160
# From node 5:
mongod --shardsvr --replSet "shard4" --dbpath ../../data/data2 --fork --logpath ../../log/data2/mongod.log --port 27024 --bind_ip 192.168.56.163
```
Connect to any one of the running mongod instance, port 27024 and run the following.
```
mongo --host 192.168.56.159 --port 27024
```
and run the following code
```
rs.initiate(
  {
    _id : "shard4",
    members: [
      { _id : 0, host : "192.168.56.159:27024" },
      { _id : 1, host : "192.168.56.160:27024" },
      { _id : 4, host : "192.168.56.163:27024" }
    ]
  }
)
```
##### e. For shard5 run the following in respective nodes
```
# From node 1:
mongod --shardsvr --replSet "shard5" --dbpath ../../data/data3 --fork --logpath ../../log/data3/mongod.log --port 27025 --bind_ip 192.168.56.159
# From node 4:
mongod --shardsvr --replSet "shard5" --dbpath ../../data/data3 --fork --logpath ../../log/data3/mongod.log --port 27025 --bind_ip 192.168.56.162
# From node 5:
mongod --shardsvr --replSet "shard5" --dbpath ../../data/data3 --fork --logpath ../../log/data3/mongod.log --port 27025 --bind_ip 192.168.56.163
```
Connect to any one of the running mongod instance, port 27025 and run the following.
```
mongo --host 192.168.56.159 --port 27025
```
and run the following code
```
rs.initiate(
  {
    _id : "shard5",
    members: [
      { _id : 0, host : "192.168.56.159:27025" },
      { _id : 3, host : "192.168.56.162:27025" },
      { _id : 4, host : "192.168.56.163:27025" }
    ]
  }
)
```
### 4. Sharding

##### a. Connect to the mongos via mongo shell (Port# 27017)

Use the primary member as the hostname
```
mongo 192.168.56.159:27017/admin
```

##### c. Add shards with the below commands
```
sh.addShard("shard1/192.168.56.159:27021,192.168.56.160:27021,192.168.56.161:27021")
sh.addShard("shard2/192.168.56.160:27022,192.168.56.161:27022,192.168.56.162:27022")
sh.addShard("shard3/192.168.56.161:27023,192.168.56.162:27023,192.168.56.163:27023")
sh.addShard("shard4/192.168.56.159:27024,192.168.56.160:27024,192.168.56.163:27024")
sh.addShard("shard5/192.168.56.159:27025,192.168.56.162:27025,192.168.56.163:27025")
```

##### d. Check the status of the shard. There should be three hostnames in each 'shards' field.
```
sh.status()
```

### 5. Prepare directory
Before running the scripts, make sure the project folder is uploaded into the home folder. 
Change directory to the project folder to prepare for benchmarking.
```
cd MongoDbPerformance-CS5424
```
Copy the project-files into this directory
```
cp -R <project-files path> ./project-files
```

(Optional) Rebuild the maven package. Optional as the project files combines with the built binaries.
```
mvn clean dependency:copy-dependencies package
```

### 6. Run Experiments
To run an experiment
```
java -cp target/*:target/dependency/*:. assign2.Experiment <NC> <read_concern> <write_concern>
```
<NC> is an integer for the number of clients
<read_concern> is either 'majority' or 1 (for local)
<write_concern> is either 'majority' or 1 or 3

It would first load (or reload) the data, then spawn NC clients as processes.

To run for all experiments sequentially
```
sh run_all_experiments.sh
```

### 7. Evaluate performance

After running the experiment/s:

The transaction logs are stored in ./experiment__nc_<NC>__r_<read_concern>__w_<write_concern>/xact-output/<i>-output.txt
The client performance report is scored in ./experiment__nc_ <NC>__r_<read_concern>__w_<write concern>/report/<i>-report.txt
After all transactions are completed:
 - the throughput statistics across clients are calculated and stored in ./experiment__nc_<NC>__r_<read_concern>__w_<write_concern>/report/throughput-report.txt.
 - the data base state is calculated and stored in ./experiment__nc_<NC>__r_<read_concern>__w_<write_concern>/report/state-report.txt.
