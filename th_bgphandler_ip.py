import time

import pybgpstream
import redis
from concurrent.futures import ThreadPoolExecutor, as_completed
# used_memory_human:27.91M
# init bgpStream
bgpStream = pybgpstream.BGPStream(record_type='ribs')
bgpStream.stream.set_data_interface('singlefile')
bgpStream.stream.set_data_interface_option('singlefile', 'rib-file', 'route-views.linx.bz2')
bgpStream.stream.set_data_interface_option('singlefile', 'rib-type', 'mrt')
bgpStream.stream.add_interval_filter(0, 0)

# init redis
redisPool = redis.ConnectionPool(host='localhost', port=6379, db=1)
rp = redis.Redis(connection_pool=redisPool)

nodeAll = 'node_all_{:s}'
node_prefix = 'node_asn_{:s}_{:s}'


# handler
# for elem in bgpStream:
#    print(elem)
#    ases = elem.fields["as-path"].split(" ")
#    if len(ases)==1 :
#       continue
#    print(ases)
#    pipe=rp.pipeline()
#    pipe.sadd(nodeAll,*ases)
#    for i,val in enumerate(list, 1):
#       pipe.sadd(node_prefix.format(ases[i-1]),val)
#    pipe.execute()

def handle(elems):
    pipe = rp.pipeline()
    for elem in elems:
        ases = elem.fields["as-path"].split(" ")
        prefix = elem.fields['prefix']
        if len(ases) == 1:
            return
        ver = "v4"
        if prefix.find(":") != -1:
            ver = "v6"
        pipe.sadd(nodeAll.format(ver), *ases)
        for i in range(0, len(ases) - 1):
            pipe.sadd(node_prefix.format(ver, ases[i]), ases[i + 1])
    pipe.execute()
    return


t=ThreadPoolExecutor(max_workers=30)
count=0
last=0
elems=[]
for elem in bgpStream:
    count+=1
    print(elem)
    print(count)
    elems.append(elem)
    if len(elems) >20 :
        t.submit(handle,elems)
        elems=[]
t.shutdown()
print(time.time())