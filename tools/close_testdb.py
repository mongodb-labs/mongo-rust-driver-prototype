import subprocess

f = open('tools/.shard.tmp', 'r').readlines()

for line in f:
    try:
        subprocess.check_output(['kill', '-9', line.strip()])
    except:
        pass

subprocess.check_output("rm -rf tools/s1".split(' '))
subprocess.check_output("rm -rf tools/s2".split(' '))
subprocess.check_output("rm -rf tools/cfg1".split(' '))
subprocess.check_output("rm -rf tools/cfg2".split(' '))
subprocess.check_output("rm -rf tools/cfg3".split(' '))
subprocess.check_output("rm -rf tools/*.log".split(' '))
subprocess.check_output("rm -rf tools/mongos.log*".split(' '))
subprocess.check_output("rm -rf tools/.shard.tmp".split(' '))
