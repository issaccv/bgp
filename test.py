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

def handle(elem):
    ases = elem.fields["as-path"].split(" ")
    if len(ases) == 1:
        return
    pipe = rp.pipeline()
    pipe.sadd(nodeAll, *ases)
    for i in range(0,len(ases)-1):
        pipe.sadd(node_prefix.format(ases[i]), ases[i+1])
    pipe.execute()
    return

count=0
for elem in bgpStream:
    print(elem)
    print(++count)
    handle(elem)
