from uhashring import HashRing
#list of slices a hostname is part of
host_slice = {}
slice_host = {}

#list of slice consistent hashes
slice_list = {}

#map of slice to a list of requests in it.
req_list = {}

#map of host to its weight
host_weight = {}


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
        hostval['weight'] = 1+len(slice_list) - host_weight[hostval['nodename']]

def redistribute():
    #print("redistributing")
    for ring in slice_list.values():
        syncweight(ring)
        ring.regenerate()
        #print(ring.nodes)


def add_slice(sliceid, hostlist):
    slice_host[sliceid] = []
    req_list[sliceid] = []
    for host in hostlist:
        if host not in host_slice:
            add_host(host)
        host_slice[host].append(sliceid)
        slice_host[sliceid].append(host)
        host_weight[host] = len(host_slice[host])
        #print("hostwe:" + str(host_weight[host]))
    #slice_list[sliceid] = HashRing(hostlist, vnodes=40, replicas=0, weight_fn= weight_fn)
    slice_list[sliceid] = HashRing(hostlist, replicas=0)
    redistribute()


def rem_slice(sliceid):
    if sliceid not in slice_list:
        print("No slice present with slice id " + sliceid )
        return
    for host in slice_host[sliceid]:
        host_slice[host].remove(sliceid)
        host_weight[host] = len(host_slice[host])
    del slice_list[sliceid]
    del slice_host[sliceid]
    #time to redistribute
    redistribute()


def add_host_slice( hostname, sliceid):
    if hostname not in host_slice:
        add_host(hostname)
    if sliceid not in slice_list:
        add_slice(sliceid, [hostname])
        return
    #Todo:Add check to remove duplicates
    slice_host[sliceid].append(hostname)
    ring = slice_list[sliceid]
    ring.add_node(hostname)
    #now that a new host is added into a slice, we need to re weigh all the hash rings
    redistribute()


def rem_host_slice(hostname, sliceid):
    if hostname not in host_slice:
        print("Not a valid hostname: " + hostname)
        return
    if sliceid not in slice_list:
        print("Not a valid slice id: " + sliceid)
        return
    if hostname not in slice_host[sliceid]:
        print("Host name not present in slice")
        return

    #Now if this hostname is the only one in the slice then the slice must be removed
    if len(slice_host[sliceid]) == 1:
        rem_slice(sliceid)
        return

    ring = slice_list[sliceid]
    ring.remove_node(hostname)
    slice_host[sliceid].remove(hostname)
    host_slice[hostname].remove(sliceid)
    host_weight[hostname] = len(host_slice[hostname])
    redistribute()

def rem_host(hostname):
    if hostname not in host_slice:
        print("Hostname not present :" + hostname )
        return
    for slice in host_slice[hostname]:
        #print(slice)
        rem_host_slice(hostname,slice)
    del host_slice[hostname]
    del host_weight[hostname]


def req(sess_id, sliceid):
    if sliceid not in slice_list:
        print("slice id not present")
        return
    #now let us find out the location of the host and add entry there
    ring = slice_list[sliceid]
    req_list[sliceid].append(sess_id)


def getmappedhost(ring, key):
    return ring.get_node(key)


def distribution(sliceid):
    disp = {}
    ring = slice_list[sliceid]
    for val in ring.get_nodes():
        disp[val] = []
    for req in req_list[sliceid]:
        disp[getmappedhost(ring, req)].append(req)
    print(disp)

def distcount(sliceid):
    disp = {}
    ring = slice_list[sliceid]
    for val in ring.get_nodes():
        disp[val] = 0
    for req in req_list[sliceid]:
        disp[getmappedhost(ring, req)] += 1
    print(disp)

def printweights(sliceid):
    ring = slice_list[sliceid]
    print(ring.nodes)


def test():
    add_host("nodeA")
    add_host("nodeB")
    add_host("nodeC")
    add_slice("slice1",["nodeA", "nodeB"])
    add_slice("slice2",["nodeA", "nodeC"])
    printweights("slice1")
    printweights("slice2")

    for i in range(1, 100):
        req(str(i), "slice2")

    for i in range(100, 200):
        req(str(i), "slice1")


    print(req_list)
    for slice in slice_list:
        print(slice)
        distcount(slice)

    #remove hostB
    print("Removing nodeB")
    rem_host('nodeB')
    for slice in slice_list:
        print(slice)
        distcount(slice)
    print("removing slice1")
    rem_slice('slice1')
    for slice in slice_list:
        print(slice)
        distcount(slice)
test()

