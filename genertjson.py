import  json

import redis

class node:
    def __init__(self,name):
        self.name=name


class link:
    def __init__(self,source,target):
        self.source = source
        self.target = target

class Graph :
    def __init__(self,ty,nodes,links):
        self.type=ty
        self.nodes=nodes
        self.links=links

# init redis
redisPool = redis.ConnectionPool(host='localhost', port=6379, db=0)
rp = redis.Redis(connection_pool=redisPool)

keys=rp.keys("node_asn_*")
node_prefix = 'node_asn_{:s}'

pipe=rp.pipeline()
for key in keys :
    pipe.smembers(keys)
result=pipe.execute()

ty="force"
nodes=keys
links=[]

for i in range(0, len(result)):
    for val in  result[i].values():
        links.append(link(keys[i],node_prefix.format(val)))

g=Graph(ty,nodes,links)
with open('data.json', 'w') as f:
    json.dump(g, f)