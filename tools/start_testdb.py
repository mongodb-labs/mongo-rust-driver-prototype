import subprocess

f = open('tools/.shard.tmp', 'w')
procs = []

def procnum(st):
    s = st[st.index('forked process:') + 14:].strip()
    return str(s[2:7])

### Create directories
subprocess.check_output("mkdir -p tools/s1".split(' '))
subprocess.check_output("mkdir -p tools/s2".split(' '))
subprocess.check_output("mkdir -p tools/cfg1".split(' '))
subprocess.check_output("mkdir -p tools/cfg2".split(' '))
subprocess.check_output("mkdir -p tools/cfg3".split(' '))

### Start MongoDB instances
procs.append(procnum(subprocess.check_output(
        "mongod --port 27017 --dbpath tools/s1 --logpath s1.log --fork --shardsvr".split(' '))))
procs.append(procnum(subprocess.check_output(
        "mongod --port 37017 --dbpath tools/s2 --logpath s2.log --fork --shardsvr".split(' '))))

### Start config servers
procs.append(procnum(subprocess.check_output(
        "mongod --port 47017 --dbpath tools/cfg1 --logpath tools/cfg1.log --fork --configsvr".split(' '))))
procs.append(procnum(subprocess.check_output(
        "mongod --port 47018 --dbpath tools/cfg2 --logpath tools/cfg2.log --fork --configsvr".split(' '))))
procs.append(procnum(subprocess.check_output(
        "mongod --port 47019 --dbpath tools/cfg3 --logpath tools/cfg3.log --fork --configsvr".split(' '))))

### Start shard controller
procs.append(procnum(subprocess.check_output(
    "mongos --port 57017 --logpath tools/mongos.log --fork --configdb localhost:47017,localhost:47018,localhost:47019".split(' '))))
for pn in procs:
    f.write(pn + '\n')
