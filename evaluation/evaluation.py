from collections import defaultdict
import sys


def get_stats(logs):
    consistency_percentages = dict()
    for node, log in logs:
        with open(log, 'r') as f:
            downloads = set()
            requests = 0
            failed = 0
            updates = 0
            invalid = 0
            for line in f.readlines():
                if 'FAIL' in line:
                    failed += 1
                    continue
                if line[0] != '!':
                    continue
                data = line.split()
                if 'OBTN' in line:
                    requests += 1
                    if data[-1] in downloads:
                        updates += 1
                    downloads.add(data[-1])
                elif 'SRCH' in line:
                    requests += 1
                    res = ['[{}/{}]'.format(i, data[-2][1:-1]) for i in data[-1][1:-1].split(',')]
                    if any(r in downloads for r in res):
                        if node not in data[-1]:
                            invalid += 1
                elif 'RMV' in line:
                    if data[-1] in downloads:
                        downloads.remove(data[-1])
                    else:
                        invalid += 1
        
        consistency_percentages[node] = (requests, failed, updates, invalid)
    return consistency_percentages


push_1 = get_stats([(str(55010), "push/1/55010_client.log")])
push_2 = get_stats([(str(i+55000), "push/2/55{:03d}_client.log".format(i)) for i in range(10, 12)])
push_3 = get_stats([(str(i+55000), "push/3/55{:03d}_client.log".format(i)) for i in range(10, 13)])
push_4 = get_stats([(str(i+55000), "push/4/55{:03d}_client.log".format(i)) for i in range(10, 14)])
push_5 = get_stats([(str(i+55000), "push/5/55{:03d}_client.log".format(i)) for i in range(10, 15)])
push_6 = get_stats([(str(i+55000), "push/6/55{:03d}_client.log".format(i)) for i in range(10, 16)])
push_7 = get_stats([(str(i+55000), "push/7/55{:03d}_client.log".format(i)) for i in range(10, 17)])
push_8 = get_stats([(str(i+55000), "push/8/55{:03d}_client.log".format(i)) for i in range(10, 18)])
push_9 = get_stats([(str(i+55000), "push/9/55{:03d}_client.log".format(i)) for i in range(10, 19)])
push_10 = get_stats([(str(i+55000), "push/10/55{:03d}_client.log".format(i)) for i in range(10, 20)])
print 'PUSH STATS'
print '1 client:', push_1, sum(p[3] / float(p[0]) for p in push_1.values()) / len(push_1.values())
print '2 clients:', push_2, sum(p[3] / float(p[0]) for p in push_2.values()) / len(push_2.values())
print '3 clients:', push_3, sum(p[3] / float(p[0]) for p in push_3.values()) / len(push_3.values())
print '4 clients:', push_4, sum(p[3] / float(p[0]) for p in push_4.values()) / len(push_4.values())
print '5 client:', push_5, sum(p[3] / float(p[0]) for p in push_5.values()) / len(push_5.values())
print '6 clients:', push_6, sum(p[3] / float(p[0]) for p in push_6.values()) / len(push_6.values())
print '7 clients:', push_7, sum(p[3] / float(p[0]) for p in push_7.values()) / len(push_7.values())
print '8 clients:', push_8, sum(p[3] / float(p[0]) for p in push_8.values()) / len(push_8.values())
print '9 client:', push_9, sum(p[3] / float(p[0]) for p in push_9.values()) / len(push_9.values())
print '10 clients:', push_10, sum(p[3] / float(p[0]) for p in push_10.values()) / len(push_10.values())
print '-'*32

pull_n_1 = get_stats([(str(i+55000), "pull_n/1/55{:03d}_client.log".format(i)) for i in range(10, 12)])
pull_n_2 = get_stats([(str(i+55000), "pull_n/2/55{:03d}_client.log".format(i)) for i in range(10, 12)])
pull_n_3 = get_stats([(str(i+55000), "pull_n/3/55{:03d}_client.log".format(i)) for i in range(10, 12)])
pull_n_4 = get_stats([(str(i+55000), "pull_n/4/55{:03d}_client.log".format(i)) for i in range(10, 12)])
pull_n_5 = get_stats([(str(i+55000), "pull_n/5/55{:03d}_client.log".format(i)) for i in range(10, 12)])
pull_n_6 = get_stats([(str(i+55000), "pull_n/6/55{:03d}_client.log".format(i)) for i in range(10, 12)])
pull_n_7 = get_stats([(str(i+55000), "pull_n/7/55{:03d}_client.log".format(i)) for i in range(10, 12)])
pull_n_8 = get_stats([(str(i+55000), "pull_n/8/55{:03d}_client.log".format(i)) for i in range(10, 12)])
pull_n_9 = get_stats([(str(i+55000), "pull_n/9/55{:03d}_client.log".format(i)) for i in range(10, 12)])
pull_n_10 = get_stats([(str(i+55000), "pull_n/10/55{:03d}_client.log".format(i)) for i in range(10, 12)])
print 'PULL FROM NODES STATS'
print 'TTR 1 sec:', pull_n_1, sum(p[3] / float(p[0]) for p in pull_n_1.values()) / len(pull_n_1.values())
print 'TTR 2 sec:', pull_n_2, sum(p[3] / float(p[0]) for p in pull_n_2.values()) / len(pull_n_2.values())
print 'TTR 3 sec:', pull_n_3, sum(p[3] / float(p[0]) for p in pull_n_3.values()) / len(pull_n_3.values())
print 'TTR 4 sec:', pull_n_4, sum(p[3] / float(p[0]) for p in pull_n_4.values()) / len(pull_n_4.values())
print 'TTR 5 sec:', pull_n_5, sum(p[3] / float(p[0]) for p in pull_n_5.values()) / len(pull_n_5.values())
print 'TTR 6 sec:', pull_n_6, sum(p[3] / float(p[0]) for p in pull_n_6.values()) / len(pull_n_6.values())
print 'TTR 7 sec:', pull_n_7, sum(p[3] / float(p[0]) for p in pull_n_7.values()) / len(pull_n_7.values())
print 'TTR 8 sec:', pull_n_8, sum(p[3] / float(p[0]) for p in pull_n_8.values()) / len(pull_n_8.values())
print 'TTR 9 sec:', pull_n_9, sum(p[3] / float(p[0]) for p in pull_n_9.values()) / len(pull_n_9.values())
print 'TTR 10 sec:', pull_n_10, sum(p[3] / float(p[0]) for p in pull_n_10.values()) / len(pull_n_10.values())
print '-'*32

pull_p_1 = get_stats([(str(i+55000), "pull_p/1/55{:03d}_client.log".format(i)) for i in range(10, 12)])
pull_p_2 = get_stats([(str(i+55000), "pull_p/2/55{:03d}_client.log".format(i)) for i in range(10, 12)])
pull_p_3 = get_stats([(str(i+55000), "pull_p/3/55{:03d}_client.log".format(i)) for i in range(10, 12)])
pull_p_4 = get_stats([(str(i+55000), "pull_p/4/55{:03d}_client.log".format(i)) for i in range(10, 12)])
pull_p_5 = get_stats([(str(i+55000), "pull_p/5/55{:03d}_client.log".format(i)) for i in range(10, 12)])
pull_p_6 = get_stats([(str(i+55000), "pull_p/6/55{:03d}_client.log".format(i)) for i in range(10, 12)])
pull_p_7 = get_stats([(str(i+55000), "pull_p/7/55{:03d}_client.log".format(i)) for i in range(10, 12)])
pull_p_8 = get_stats([(str(i+55000), "pull_p/8/55{:03d}_client.log".format(i)) for i in range(10, 12)])
pull_p_9 = get_stats([(str(i+55000), "pull_p/9/55{:03d}_client.log".format(i)) for i in range(10, 12)])
pull_p_10 = get_stats([(str(i+55000), "pull_p/10/55{:03d}_client.log".format(i)) for i in range(10, 12)])
print 'PULL FROM PEERS STATS'
print 'TTR 1 sec:', pull_p_1, sum(p[3] / float(p[0]) for p in pull_p_1.values()) / len(pull_p_1.values())
print 'TTR 2 sec:', pull_p_2, sum(p[3] / float(p[0]) for p in pull_p_2.values()) / len(pull_p_2.values())
print 'TTR 3 sec:', pull_p_3, sum(p[3] / float(p[0]) for p in pull_p_3.values()) / len(pull_p_3.values())
print 'TTR 4 sec:', pull_p_4, sum(p[3] / float(p[0]) for p in pull_p_4.values()) / len(pull_p_4.values())
print 'TTR 5 sec:', pull_p_5, sum(p[3] / float(p[0]) for p in pull_p_5.values()) / len(pull_p_5.values())
print 'TTR 6 sec:', pull_p_6, sum(p[3] / float(p[0]) for p in pull_p_6.values()) / len(pull_p_6.values())
print 'TTR 7 sec:', pull_p_7, sum(p[3] / float(p[0]) for p in pull_p_7.values()) / len(pull_p_7.values())
print 'TTR 8 sec:', pull_p_8, sum(p[3] / float(p[0]) for p in pull_p_8.values()) / len(pull_p_8.values())
print 'TTR 9 sec:', pull_p_9, sum(p[3] / float(p[0]) for p in pull_p_9.values()) / len(pull_p_9.values())
print 'TTR 10 sec:', pull_p_10, sum(p[3] / float(p[0]) for p in pull_p_10.values()) / len(pull_p_10.values())
print '-'*32