#!/usr/bin/python
import sys,os,subprocess,shlex,socket,warnings
import yaml,MySQLdb
import daemon,lockfile,cmd

# getting warnings about deprec libs...
warnings.filterwarnings('ignore')
import paramiko
warnings.filterwarnings('always')

#target/assumptions:  for use on ec2 databases for failover, in absence of IPs that
# can be reallocated (ignoring elastic IPs for now).  We have two machines in
# master-master replication, named "db1" and "db2"
#TODO: check to see if other nat rules exist.  Delete just the one added by this.

#command used to get the host name from a mysql db
myDBHost = "select * from INFORMATION_SCHEMA.GLOBAL_VARIABLES where VARIABLE_NAME = 'hostname';"

#load database conn variables from yaml
cfgfile = '/EBS1/www/dcshoes/site.cfg'
cfg = open(cfgfile,'r').read()
o = yaml.load(cfg)

#...why do I have to define this? hy doesn't it just use what's there?
#NOTE: that said, this will be using something other than my stuff as a daemon
privKeyFile = os.path.expanduser('~/.ssh/id_rsa')
privKey = paramiko.RSAKey.from_private_key_file(privKeyFile)
sshUser = 'brian'

dbhost = o['db_host']
dbname = o['db_name']
dbuser = o['db_user']
dbpass = o['db_pass']

#NOTE: should these be updated frequently for nameservice change updates?
db1Ip = socket.gethostbyname('db1')
db2Ip = socket.gethostbyname('db2')


def conn_direct():
    # test conn to 3306, to see if it's what you think
    conn = MySQLdb.connect (host = o['db_host'],
                            user = o['db_user'],
                            passwd = o['db_pass'])
    cursor = conn.cursor ()
    #NOTE: change to more extensive test, if desired
    cursor.execute (myDBHost)
    dirResp = cursor.fetchone()[1]
    #TODO: catch errors
    dirErr = ''
    cursor.close ()
    conn.close ()
    return (dirResp,dirErr)

def conn_via_ssh():
    makeSsh = paramiko.SSHClient()
    makeSsh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    makeSsh.connect('db1', username = sshUser, pkey = privKey)
    myCmd = 'echo "'+myDBHost+'"|mysql -u %s -p%s' % (o['db_user'], o['db_pass'])
    stdin, stdout, stderr = makeSsh.exec_command(myCmd)
    stdin.close()
    sshResp = stdout.readlines()[1].split("\t")[1]
    sshErr = stderr.readlines()
    makeSsh.close()
    return (sshResp,sshErr)

def add_rule():
    addRuleCmd = 'sudo iptables -t nat -A OUTPUT -p tcp -d %s --dport 3306 -j NETMAP --to %s' % (db1Ip, db2Ip)
    subprocess.call(addRuleCmd, shell=True)

def del_rule():
    # note we'd need to refine a lot of this if other nat rules exist.
    delRuleCmd = 'sudo iptables -t nat -F'
    subprocess.call(delRuleCmd, shell=True)

def the_results(runNote):
    dirResp,dirErr = conn_direct()
    sshResp,sshErr = conn_via_ssh()
    print "(%s) Direct: %s, via-ssh: %s" % (runNote, dirResp, sshResp)

if __name__ == "__main__":
#brian@dcshoes-app1:~/dev_bin/ec2-dbfailover$ sudo iptables -t nat -n -L|grep NETMAP
#NETMAP     tcp  --  0.0.0.0/0            10.252.186.244      tcp dpt:3306 10.252.187.196/32

    ruleFindCmd1 = 'sudo iptables -t nat -n -L'
    rFC1args = shlex.split(ruleFindCmd1)
    ruleFindCmd2 = 'grep NETMAP'
    rFC2args = shlex.split(ruleFindCmd2)
    rf1 = Popen([rFC1args], stdout=PIPE)
    rf2 = Popen([rFC2args], stdin=rf1.stdout, stdout=PIPE)
    ruleFindResp,ruleFindErr = Popen(ruleFindCmd, stdout=PIPE, stderr
    #daemonContext = daemon.DaemonContext(pidfile=lockfile.FileLock('(/var/run/dbcheck.pid'))
    #with daemonContext:
    #    conn_direct()
        #if rule exists:
        #   try:
        #       conn  - PROBLEM:  if rule exists, you can't reach db1.   Make redirect?
        #   if conn
        #     remove rule (iptables -t nat -F)
        #else:
        #   try:
        #       conn
        #   if conn
        #       add rule
