import subprocess

f = open('tools/.shard.tmp', 'w')
procs = []

def procnum(st):
    s = st[st.index('forked process:') + 14:].strip()
    return str(s[2:7])
def open_db(st):
    s = subprocess.check_output(st.split(' '))
    print s
    procs.append(procnum(s))

### Create directories
subprocess.check_output("mkdir -p tools/s1".split(' '))
subprocess.check_output("mkdir -p tools/s2".split(' '))
subprocess.check_output("mkdir -p tools/cfg1".split(' '))
subprocess.check_output("mkdir -p tools/cfg2".split(' '))
subprocess.check_output("mkdir -p tools/cfg3".split(' '))

### Start MongoDB instances
open_db("mongod --port 27017 --dbpath tools/s1 --logpath tools/s1.log --fork --shardsvr")
open_db("mongod --port 37017 --dbpath tools/s2 --logpath tools/s2.log --fork --shardsvr")

### Start config servers
open_db("mongod --port 47017 --dbpath tools/cfg1 --logpath tools/cfg1.log --fork --configsvr")
open_db("mongod --port 47018 --dbpath tools/cfg2 --logpath tools/cfg2.log --fork --configsvr")
open_db("mongod --port 47019 --dbpath tools/cfg3 --logpath tools/cfg3.log --fork --configsvr")

### Start shard controller
#FIXME: this only works if you run it twice for unknown reasons
open_db("mongos --port 57017 --fork --logpath tools/mongos.log --configdb localhost:47017,localhost:47018,localhost:47019")
open_db("mongos --port 57017 --fork --logpath tools/mongos.log --configdb localhost:47017,localhost:47018,localhost:47019")

for pn in procs:
    f.write(pn + '\n')
