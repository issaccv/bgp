import time

import pybgpstream
import redis
from concurrent.futures import ThreadPoolExecutor, as_completed
# used_memory_human:27.91M
# init bgpStream
bgpStream = pybgpstream.BGPStream(record_type='ribs')
bgpStream.stream.set_data_interface('singlefile')
bgpStream.stream.set_data_interface_option('singlefile', 'rib-file', "rib.20190606.1400.bz2")
bgpStream.stream.set_data_interface_option('singlefile', 'rib-type', 'mrt')
bgpStream.stream.add_interval_filter(0, 0)

# init redis
redisPool = redis.ConnectionPool(host='localhost', port=6379, db=12)
rp = redis.Redis(connection_pool=redisPool)

nodeAll = 'node_all'
node_prefix = 'node_asn_{:s}'


def handle(elems):
    pipe = rp.pipeline()
    for elem in elems:
        aspath = elem.fields["as-path"]
        ases = elem.fields["as-path"].split(" ")
        prefix = elem.fields['prefix']
        pipe.hset(node_prefix.format(ases[0]),prefix,aspath)
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
    if len(elems) >10 :
        t.submit(handle,elems)
        elems=[]
t.shutdown()
print(time.time())