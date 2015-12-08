#!/usr/bin/python
# -*- coding: utf-8 -*-

import socket
import sys
from rdbtools import RdbParser, RdbCallback
import redis


SRC_REDIS_IP = '127.0.0.1'
SRC_REDIS_PORT = 6379
TGT_REDIS_IP = '127.0.0.1'
TGT_REDIS_PORT = 7000
BUFFER_SIZE = 1024
RDB_FILENAME = "sync-py.rdb"

target = redis.StrictRedis(host=TGT_REDIS_IP, port=TGT_REDIS_PORT, db=0)

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

def read_update(s):
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
            
        handle_command(s, command)

def load_rdb():
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
            target.sadd(key, member)

        def rpush(self, key, value) :
            print('%s has [%s]' % (str(key), str(value)))

        def zadd(self, key, score, member):
            print('%s has {%s : %s}' % (str(key), str(member), str(score)))

    callback = MyCallback()
    parser = RdbParser(callback)
    parser.parse(RDB_FILENAME)

def handle_command(s, command):
    # print "handle_command:", command
    if command[0] == "PING":
        s.send("+PONG\r\n")
    if command[0].upper() == "SADD":
        target.sadd(command[1], command[2])

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((SRC_REDIS_IP, SRC_REDIS_PORT))
s.send("PING\r\n")
pong = s.recv(BUFFER_SIZE)
if not pong.startswith("+PONG"):
    print "ping failed", pong
    s.close()
    sys.exit()

s.send("SYNC\r\n")
read_bulk(s)


load_rdb()

while True:
    read_update(s)

s.close()

#test cases:
#1. 用一个给定的 rdb 启动一个测试 redis-server，运行 sync.py，sync.py 产生的 rdb 文件跟给的的 rdb 相同；
#2. 操作一个新的 key，sync.py 能收到正常的操作同步；
#3. redis-server 执行 bgsave，重新运行 sync.py，sync.py 产生的 rdb 文件与 bgsave 更新后的 rdb 文件相同；
#4. sync.py 持续运行几分钟不会断线；
#5. 持续运行几分钟后，很能收到操作同步；
