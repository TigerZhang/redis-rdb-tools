#!/usr/bin/python
# -*- coding: utf-8 -*-

import socket
import sys
from rdbtools import RdbParser, RdbCallback
import redis
import getopt

# SRC_REDIS_IP = '127.0.0.1'
# SRC_REDIS_PORT = 6379
# TGT_REDIS_IP = '127.0.0.1'
# TGT_REDIS_PORT = 7000
BUFFER_SIZE = 1024
# RDB_FILENAME = "sync-py.rdb"
# appkeys = ['564c13b8f085fc471efdfff8']

def connect_target(target_redis_ip, target_redis_port):
    return redis.StrictRedis(host=target_redis_ip, port=target_redis_port, db=0)

def get_size(s):
    c = s.recv(1)
    size = c
    while True:
        c = s.recv(1)
        if c == '\n':
            break
        size += c
    return size

def read_bulk(s, rdb_filename):
    s.send("SYNC\r\n")
    c = s.recv(1)
    if c.startswith('$'):
        len = int(get_size(s))
        if len > BUFFER_SIZE:
            print "large bulk with size " + str(len) + " should write to file"
        f = open(rdb_filename, 'wb')
        while True:
            curlen = min(BUFFER_SIZE, len)
            buf = s.recv(curlen)
            f.write(buf)
            if curlen == len:
                f.close()
                break
            len -= curlen

def read_update(s, target, appkeys):
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
            
        handle_command(s, target, command, appkeys)

def set_op_wrapper(target, cmd, key, member, appkeys):
    for i, appkey in enumerate(appkeys):
        if appkey in key:
            print "apply:", cmd, key, member
            getattr(target, cmd)(key, member)
        else:
            i = i
            # print "ignore:", cmd, key, member

def load_rdb(target, rdb_filename, appkeys):
    class MyCallback(RdbCallback) :
        ''' Simple example to show how callback works. 
            See RdbCallback for all available callback methods.
            See JsonCallback for a concrete example
        ''' 
        def set(self, key, value, expiry):
            key = key
            # print('%s = %s' % (str(key), str(value)))

        def hset(self, key, field, value):
            key = key
            # print('%s.%s = %s' % (str(key), str(field), str(value)))

        def sadd(self, key, member):
            # print('%s has {%s}' % (str(key), str(member)))
            # target.sadd(key, member)
            set_op_wrapper(target, "sadd", key, member, appkeys)

        def rpush(self, key, value) :
            key = key
            # print('%s has [%s]' % (str(key), str(value)))

        def zadd(self, key, score, member):
            key = key
            # print('%s has {%s : %s}' % (str(key), str(member), str(score)))

    callback = MyCallback()
    parser = RdbParser(callback)
    parser.parse(rdb_filename)

def handle_command(s, target, command, appkeys):
    if command[0] == "PING":
        s.send("+PONG\r\n")
    else:
        cmd = command[0].lower()
        print "cmd:", cmd
        if cmd == "sadd" or cmd == "srem":
            # getattr(target, cmd)(command[1], command[2])
            set_op_wrapper(target, cmd, command[1], command[2], appkeys)
        else:
            print "ignore command:", command

def connect_upstream(src_redis_ip, src_redis_port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((src_redis_ip, src_redis_port))
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
    rdb_filename = 'sync-py.rdb'
    appkeys = ['564c13b8f085fc471efdfff8']

    src_redis_ip = src_redis_port = target_redis_port = target_redis_ip = None

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
            src_redis_ip = a
        elif o in ("-p", "--sourceport"):
            src_redis_port = int(a)
        elif o in ("-t", "--targetserver"):
            target_redis_ip = a
        elif o in ("-P", "--targetport"):
            target_redis_port = int(a)
        elif o in ("-r", "--redbfilename"):
            rdb_filename = a
        elif o in ("-a", "--appkeys"):
            appkeys = a.split('/')
        else:
            assert False, "unhandled option"
            # ...

    if src_redis_ip is None or src_redis_port is None or target_redis_ip is None or target_redis_port is None:
        usage()
        sys.exit(3)

    s = connect_upstream(src_redis_ip, src_redis_port)
    target = connect_target(target_redis_ip, target_redis_port)

    read_bulk(s, rdb_filename)
    load_rdb(target, rdb_filename, appkeys)

    while True:
        read_update(s, target, appkeys)

    s.close()

if __name__ == "__main__":
    main()

#test cases:
#1. 用一个给定的 rdb 启动一个测试 redis-server，运行 sync.py，sync.py 产生的 rdb 文件跟给的的 rdb 相同；
#2. 操作一个新的 key，sync.py 能收到正常的操作同步；
#3. redis-server 执行 bgsave，重新运行 sync.py，sync.py 产生的 rdb 文件与 bgsave 更新后的 rdb 文件相同；
#4. sync.py 持续运行几分钟不会断线；
#5. 持续运行几分钟后，很能收到操作同步；
