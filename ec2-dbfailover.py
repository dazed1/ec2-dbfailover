#!/usr/bin/python
import sys,os,subprocess,shlex,socket,warnings
import yaml,MySQLdb
import daemon,lockfile,cmd
from subprocess import PIPE,Popen
import logging
import time
import datetime
from getpass import getuser

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
PIDFILE='dbcheck.pid'
MYSQL_CONNECT_TIMEOUT=1 # seconds for mysql to timeout
SSH_CONNECT_TIMEOUT=1

#load database conn variables from yaml
cfgfile = '/EBS1/www/dcshoes/site.cfg'
cfg = open(cfgfile,'r').read()
o = yaml.load(cfg)

#...why do I have to define this? hy doesn't it just use what's there?
#NOTE: that said, this will be using something other than my stuff as a daemon
privKeyFile = os.path.expanduser('~/.ssh/id_rsa')
privKey = paramiko.RSAKey.from_private_key_file(privKeyFile)
sshUser = getuser() # Assume we want to use local user

dbhost = o['db_host']
dbname = o['db_name']
dbuser = o['db_user']
dbpass = o['db_pass']

#NOTE: should these be updated frequently for nameservice change updates?
db1Ip = socket.gethostbyname('db1')
db2Ip = socket.gethostbyname('db2')

def conn_direct():
    # test conn to 3306, to see if it's what you think
    try:
        conn = MySQLdb.connect (host = o['db_host'],
                                user = o['db_user'],
                                passwd = o['db_pass'],
                                connect_timeout=MYSQL_CONNECT_TIMEOUT)
        cursor = conn.cursor ()
        #NOTE: change to more extensive test, if desired
        cursor.execute (myDBHost)
        dirResp = cursor.fetchone()[1]
        #TODO: catch errors
        dirErr = ''
        cursor.close ()
        conn.close ()
    
        return dirResp == "db1"
    except Exception, e:
        logging.info("Received Operational Error: %s"%e)
        return False 


def conn_via_ssh():
    try:
        makeSsh = paramiko.SSHClient()
        makeSsh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        makeSsh.connect('db1', username = sshUser, pkey = privKey, timeout=SSH_CONNECT_TIMEOUT)
        myCmd = 'echo "'+myDBHost+'"|mysql -u %s -p%s' % (o['db_user'], o['db_pass'])
        stdin, stdout, stderr = makeSsh.exec_command(myCmd)
        stdin.close()
        output=stdout.readlines()
        if not len(output):
            # Does not appear to be running on remote box
            return False
        sshResp=output[1].split("\t")[1] 
        sshErr = stderr.readlines()
        logging.debug("SSH connection returned %s (%s)"%(sshResp,sshErr))
        makeSsh.close()

        return sshResp.strip() == "db1"
    except Exception,e:
        logging.info("Received Operational Error: %s"%e)
        return False 

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
    logging.debug("(%s) Direct: %s, via-ssh: %s" % (runNote, dirResp, sshResp))

def check_rule(runNote):
    ruleShow = "NETMAP     tcp  --  0.0.0.0/0            %s      tcp dpt:3306 %s/32" % (db1Ip,db2Ip)
    ruleFindCmd1 = 'sudo iptables -t nat -n -L'
    rFC1args = shlex.split(ruleFindCmd1)
    ruleFindCmd2 = 'grep NETMAP'
    rFC2args = shlex.split(ruleFindCmd2)
    rf1 = Popen(rFC1args, stdout=PIPE)
    rf2 = Popen(rFC2args, stdin=rf1.stdout, stdout=PIPE)
    ruleFindResp,ruleFindErr = rf2.communicate()
    if ruleShow in ruleFindResp:
        logging.debug("(%s) Response: "% runNote +ruleFindResp)
        return True
    else:
        logging.debug("(%s) Not found." % runNote)
        return False

def test():
    check_rule('initial')
    add_rule()
    check_rule('add_rule')
    del_rule()
    check_rule('del_rule')

def run():
    logging.info("Beginning loop")
    while 1:
        # TODO add health check and restore that fully checks restored route
        if check_rule('add_rule'):
            # Failover already occurred
            # check if original is back up  
            if conn_via_ssh(): # db 1 is back up
                logging.info("Db1 restored, deleting rule")
                # restore db1
                del_rule()
            # otherwise not up yet
            # TODO potentially verify db2 is still running as well?
        else:
            # Directly test mysql connection
            if not conn_direct():
                logging.info("Db1 not responding to mysql direct connection, adding rule")
                add_rule() 
                # TODO send oh shit message
        time.sleep(5) # Sleep interval
    logging.info("Exiting Run Loop")

if __name__ == "__main__":
    #TODO figure out how to only log for this module (not paramiko)!
    logging.basicConfig(level=logging.DEBUG, 
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename='dbcheck.log', 
                        filemode='a')

    #TODO check pid file

    # Double Fork
    try:
        pid=os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError,e:
        logging.error("Fork #1 failed: %d (%s)"%(e.errno, e.strerror))
        sys.exit(1)

    os.chdir("/")
    os.setsid()
    os.umask(0)

    try:
        pid = os.fork()
        if pid > 0:
            logging.info("Daemon PID %d" % pid)
            sys.exit(0)
    except OSError, e:
        logging.error("Fork #2 failed: %d (%s)"%(e.errno,e.strerror))
        sys.exit(1)

    #TODO write to pid file
    
    logging.info("Daemon Starting")
    run()

