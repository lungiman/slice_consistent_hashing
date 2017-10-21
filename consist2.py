from uhashring import HashRing
import statistics
#list of slices a hostname is part of
host_slice = {}
req_host = {}

#map of slice to a list of requests in it.
req_list = {}

#map of host to its weight
host_weight = {}

cum = {}

multiarg = None
multiarg_host = None
multiarg_cs = None

list_multiarg = []
g_req_list = []

def add_host(hostname):
    host_slice[hostname] = []
    host_weight[hostname] = 0


def weight_fn(**conf):
    host = conf['nodename']
    m = host_weight[host]
    #print("host:"+host+" m:"+str(m))
    # need to return Total number of slices - m
    return 1

def syncweight(ring):
    #print(ring.nodes)
    for host,hostval in ring.nodes.items():
        #print(host,hostval)
        #hostval['weight'] = 1+len(slice_list) - host_weight[hostval['nodename']]
        hostval['weight'] = 1

def redistribute():
    #print("redistributing")
    print(list_multiarg)
    for config in list_multiarg:
        locate = multiarg_cs
        for arg in config:
            locate = locate[arg]
        ring = locate
        syncweight(ring)
        ring.regenerate()
        #print(ring.nodes)


def find_host(sess_id, *rest):
    locate = multiarg_cs
    for arg in list(rest):
        locate = locate[arg]
    ring = locate
    return getmappedhost(ring, sess_id)

def req(sess_id, *rest):
    #TODO:validate if rest is proper
    #now let us find out the location of the host and add entry there
    #host = find_host(sess_id, rest)
    add_item_list(multiarg_host, sess_id, *rest)
    g_req_list.append((sess_id,list(rest)))


def getmappedhost(ring, key):
    return ring.get_node(key)


def distribution(*rest):
    locate = multiarg_host
    for arg in list(rest):
        locate = locate[arg]
    req_list = locate
    locate = multiarg_cs
    for arg in list(rest):
        locate = locate[arg]
    ring = locate
    disp = {}
    for val in ring.get_nodes():
        disp[val] = []
    for req in req_list:
        disp[getmappedhost(ring, req)].append(req)
    print(disp)


def cum_count():
    global cum
    cum = {}
    for slice in slice_list:
        print(slice)
        distcount(slice)
    print(cum)

def printweights(*rest):
    locate = multiarg_cs
    for arg in list(rest):
        locate = locate[arg]
    ring = locate
    dis = {}
    for key,val in ring.nodes.items():
        dis[key] = val['weight']
    print(dis)

def init_multiarg(*allargs):
    l = list(allargs)
    global multiarg
    global multiarg_host
    global multiarg_cs

    temp = [[]]
    temph = [[]]
    host = [[]]
    cs = HashRing([], replicas=0, vnodes=20)
    cs = [cs]
    for arg in l[::-1]:
        multiarg = []
        multiarg_host = []
        multiarg_cs = []
        multiarg.append( temp * arg )
        multiarg_host.append(temph *arg)
        multiarg_cs.append( cs * arg )
        temp = multiarg
        temph = multiarg_host
        cs = multiarg_cs
        #print(res)
    multiarg = multiarg[0]
    multiarg_host = multiarg_host[0]
    multiarg_cs = multiarg_cs[0]

def add_item_list(name, item, *rest):
    locate = name
    for arg in list(rest):
        locate = locate[arg]
    locate.append(item)

def val_multiarg_loc(name, *rest):
    locate = name
    for arg in list(rest):
        locate = locate[arg]
    return locate

def add_host_multiarg(hostname, *rest):
    locate = multiarg_cs
    for arg in list(rest):
        locate = locate[arg]
    ring = locate
    ring.add_node(hostname)

def del_host_multiarg(hostname, *rest):
    locate = multiarg_cs
    for arg in list(rest):
        locate = locate[arg]
    ring = locate
    ring.remove_node(hostname)


def add_multiarg_host(hostname, *rest):
    #validate if the rest contains sufficient arguments
    if hostname not in host_slice:
        add_host(hostname)

    add_item_list(multiarg, hostname, rest)
    #Todo:Add check to remove duplicates
    #slice_host[sliceid].append(hostname)
    host_slice[hostname].append(list(rest))
    host_weight[hostname] = len(host_slice[hostname])
    #update in the consistenthash
    add_host_multiarg(hostname, rest)
    #now that a new host is added into a slice, we need to re weigh all the hash rings
    redistribute()


def rem_multiarg_host(hostname, *rest):
    #validate if the rest contains sufficient arguments
    if hostname not in host_slice:
        return
    #Todo:Add check to remove duplicates
    #slice_host[sliceid].append(hostname)
    host_slice[hostname].remove(list(rest))
    host_weight[hostname] = len(host_slice[hostname])
    #update in the consistenthash
    del_host_multiarg(hostname, rest)
    #now that a new host is added into a slice, we need to re weigh all the hash rings
    redistribute()

def add_mulitarg_config(*rest):
    #todo:check if config already exists before adding
    list_multiarg.append(list(rest))
    redistribute()

def rem_multiarg_config(*rest):
    list_multiarg.remove(list(rest))
    redistribute()


def test1():
    init_multiarg(5,3,4)
    add_host("nodeA")
    add_host("nodeB")
    add_host("nodeC")
    add_host("nodeD")
    add_host("nodeE")

    add_mulitarg_config(1,1,1)
    add_mulitarg_config(2,2,2)
    add_mulitarg_config(1,1,3)
    add_mulitarg_config(3,1,1)

    add_host_multiarg('nodeA',1,1,1)
    add_host_multiarg('nodeB',1,1,1)
    add_host_multiarg('nodeC',2,2,2)
    add_host_multiarg('nodeC',1,1,1)
    add_host_multiarg('nodeC',1,1,3)

    distribution(1,1,1)
    for i in range(1,20):
        req(i,1,1,1)
    '''
    req(1,1,1,1)
    req(2,1,1,1)
    req(3,1,1,1)
    '''
    distribution(1,1,1)
test1()
#test()

