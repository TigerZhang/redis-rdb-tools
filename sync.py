#!/usr/bin/python
# -*- coding: utf-8 -*-

import socket
import sys
from rdbtools import RdbParser, RdbCallback
import redis
import getopt

SRC_REDIS_IP = '127.0.0.1'
SRC_REDIS_PORT = 6379
TGT_REDIS_IP = '127.0.0.1'
TGT_REDIS_PORT = 7000
BUFFER_SIZE = 1024
RDB_FILENAME = "sync-py.rdb"
appkeys = ['564c13b8f085fc471efdfff8']

def connect_target():
    return redis.StrictRedis(host=TGT_REDIS_IP, port=TGT_REDIS_PORT, db=0)

def get_size(s):
    c = s.recv(1)
    size = c
    while True:
        c = s.recv(1)
        if c == '\n':
            break
        size += c
    return size

def read_bulk(s):
    s.send("SYNC\r\n")
    c = s.recv(1)
    if c.startswith('$'):
        len = int(get_size(s))
        if len > BUFFER_SIZE:
            print "large bulk with size " + str(len) + " should write to file"
        f = open(RDB_FILENAME, 'wb')
        while True:
            curlen = min(BUFFER_SIZE, len)
            buf = s.recv(curlen)
            f.write(buf)
            if curlen == len:
                f.close()
                break
            len -= curlen

def read_update(s, target):
    star = s.recv(1)
    if star.startswith('*'):
        star_len = int(get_size(s))
    
        command = []
        while star_len > 0:
            c = s.recv(1)
            if c.startswith('$'):
                len = int(get_size(s))
                command.append(s.recv(len))
                star_len -= 1
            
        handle_command(s, target, command)

def set_op_wrapper(target, cmd, key, member):
    for i, appkey in enumerate(appkeys):
        if appkey in key:
            print "apply:", cmd, key, member
            getattr(target, cmd)(key, member)
        else:
            print "ignore:", cmd, key, member

def load_rdb(target):
    class MyCallback(RdbCallback) :
        ''' Simple example to show how callback works. 
            See RdbCallback for all available callback methods.
            See JsonCallback for a concrete example
        ''' 
        def set(self, key, value, expiry):
            print('%s = %s' % (str(key), str(value)))

        def hset(self, key, field, value):
            print('%s.%s = %s' % (str(key), str(field), str(value)))

        def sadd(self, key, member):
            # print('%s has {%s}' % (str(key), str(member)))
            # target.sadd(key, member)
            set_op_wrapper(target, "sadd", key, member)

        def rpush(self, key, value) :
            print('%s has [%s]' % (str(key), str(value)))

        def zadd(self, key, score, member):
            print('%s has {%s : %s}' % (str(key), str(member), str(score)))

    callback = MyCallback()
    parser = RdbParser(callback)
    parser.parse(RDB_FILENAME)

def handle_command(s, target, command):
    if command[0] == "PING":
        s.send("+PONG\r\n")
    else:
        cmd = command[0].lower()
        print "cmd:", cmd
        if cmd == "sadd" or cmd == "srem":
            # getattr(target, cmd)(command[1], command[2])
            set_op_wrapper(target, cmd, command[1], command[2])
        else:
            print "ignore command:", command

def connect_upstream():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((SRC_REDIS_IP, SRC_REDIS_PORT))
    s.send("PING\r\n")
    pong = s.recv(BUFFER_SIZE)
    if not pong.startswith("+PONG"):
        print "ping failed", pong
        s.close()
        sys.exit()
    return s

def usage():
    help = '''
    -s  <source_server>
    -p  <source_port>
    -t  <target_servre>
    -P  <target_port>
    -r  [rdb_filename]
    -a  appkeys
    '''
    print help

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "ho:vs:p:t:P:r:a:",
                                   ["help", "output=",
                                    "sourceserver=", "sourceport=",
                                    "targetserver=", "targetport=",
                                    "redbfilename=", "appkeys="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    output = None
    verbose = False
    for o, a in opts:
        print o, a
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-o", "--output"):
            output = a
        elif o in ("-s", "--sourceserver"):
            SRC_REDIS_IP = a
        elif o in ("-p", "--sourceport"):
            SRC_REDIS_PORT = a
        elif o in ("-t", "--targetserver"):
            TGT_REDIS_IP = a
        elif o in ("-P", "--targetport"):
            TGT_REDIS_PORT = a
        elif o in ("-r", "--redbfilename"):
            RDB_FILENAME = a
        elif o in ("-a", "--appkeys"):
            appkeys = a.split('/')
        else:
            assert False, "unhandled option"
            # ...

    s = connect_upstream()
    target = connect_target()

    read_bulk(s)
    load_rdb(target)

    while True:
        read_update(s, target)

    s.close()

if __name__ == "__main__":
    main()

#test cases:
#1. 用一个给定的 rdb 启动一个测试 redis-server，运行 sync.py，sync.py 产生的 rdb 文件跟给的的 rdb 相同；
#2. 操作一个新的 key，sync.py 能收到正常的操作同步；
#3. redis-server 执行 bgsave，重新运行 sync.py，sync.py 产生的 rdb 文件与 bgsave 更新后的 rdb 文件相同；
#4. sync.py 持续运行几分钟不会断线；
#5. 持续运行几分钟后，很能收到操作同步；
