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
redisPool = redis.ConnectionPool(host='localhost', port=6379, db=1)
rp = redis.Redis(connection_pool=redisPool)

keys=rp.keys("node_asn_v4*")
keys = [x.decode('utf-8') for x in keys]

node_prefix = 'node_asn_{:s}'

pipe=rp.pipeline()
for key in keys :
    pipe.scard(key)
result=pipe.execute()

kvdict={}
for i in range(0, len(result)):
    kvdict[keys[i]]=result[i]

d_order=sorted(kvdict.items(),key=lambda x:x[1],reverse=True)  # 按字典集合中，每一个元组的第二个元素排列。

d_order=d_order[:1000]


pipe=rp.pipeline()
for key in d_order :
    pipe.smembers(key[0])
result=pipe.execute()

ty="force"
nodes=[]
links=[]
for i in keys:
    nodes.append(node(i))

for i in range(0, len(result)):
    for val in result[i]:
        links.append(link(keys[i],node_prefix.format(val.decode())))

g=Graph(ty,nodes,links)
with open('data.json', 'w') as f:
    json.dump(g,default=lambda obj:obj.__dict__, fp=f,indent=4)


