import pybgpstream
import redis

# init bgpStream
bgpStream = pybgpstream.BGPStream(record_type='ribs')
bgpStream.stream.set_data_interface('singlefile')
bgpStream.stream.set_data_interface_option('singlefile', 'rib-file', 'route-views.amsix.bz2')
bgpStream.stream.set_data_interface_option('singlefile', 'rib-type', 'mrt')
bgpStream.stream.add_interval_filter(0, 0)

# db0:keys=25820,expires=0,avg_ttl=0
# db1:keys=496,expires=0,avg_ttl=0
#
# init redis
redisPool = redis.ConnectionPool(host='localhost', port=6379, db=0)
rp = redis.Redis(connection_pool=redisPool)

nodeAll = 'node_all'
node_prefix = 'node_asn_{:s}'
count=0

def handle(elem):
    ases = elem.fields["as-path"].split(" ")
    prefix=elem.fields['prefix']
    if prefix.find(":")!=-1:
        print(elem)
        print("v6")
        print(count)
    return

for elem in bgpStream:
    count=count+1
    handle(elem)


