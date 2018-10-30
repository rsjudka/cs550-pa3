from subprocess import call, Popen, PIPE
from sys import argv
from multiprocessing import Process
import os
import sys
import time
import random


os.chdir("../src/")


def run_peer(id_, config_dir):
    cmd = ['./super_peer', id_, config_dir]
    Popen(cmd, stdin=PIPE, stdout=open(os.devnull, 'w'))

peers = [(str(i), '../config/{}.cfg'.format(sys.argv[2])) for i in range(10)]

for peer in peers:
    p = Process(target=run_peer, args=peer)
    p.start()


time.sleep(2)

mapped_files = [(i+55010, f) for i in range(int(sys.argv[1]), 19) for f in os.listdir('nodes/n{}/local/'.format(i))]


def run_node(id_, config_dir, files_dir, does_q_d_r):
    cmd = ['./leaf_node', id_, config_dir, files_dir]
    node = Popen(cmd, stdin=PIPE if does_q_d_r else None, stdout=open(os.devnull, 'w'))

    time.sleep(5)
    if does_q_d_r:
        for _ in range(500):
            time.sleep(random.random()/2)
            req = random.choice(['s', 'o', 'r'])
            req_param = random.choice(mapped_files)
            if req == 's':
                node.stdin.write('s\n{}\n'.format(req_param[1]))
                node.stdin.flush()
            elif req == 'o':
                node.stdin.write('o\n{}\n{}\n'.format(*req_param))
                node.stdin.flush()
            elif req == 'r':
                node.stdin.write('r\n{}\n{}\n'.format(*req_param))
                node.stdin.flush()

    time.sleep(90)

nodes = [(str(i), '../config/{}.cfg'.format(sys.argv[2]), 'nodes/n{}/'.format(i), i < int(sys.argv[1])) for i in range(19)]

for node in nodes:
    p = Process(target=run_node, args=node)
    p.start()

time.sleep(3)
for _ in range(550):
    time.sleep(random.random()/2)
    rand_file = random.choice(mapped_files)
    sim_mod = open('nodes/n{}/local/{}'.format(rand_file[0]-55010, rand_file[1]), 'w')
    sim_mod.close()


time.sleep(15)

os.system('killall leaf_node')
os.system('killall super_peer')

# os.system('mv logs/leaf_nodes/* ../evaluation/{}/{}/'.format(sys.argv[2], sys.argv[1]))
# os.system('rm logs/super_peers/*')
