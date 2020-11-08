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


nodeset=set()
nodeset_exist=set()
strlist=[]
pipe=rp.pipeline()
for key in d_order :
    pipe.smembers(key[0])
    nodeset.add(key[0][12:])
    strlist.append(key[0][12:])
result=pipe.execute()

ty="force"
nodes=[]
links=[]


for i in range(0, len(result)):
    for val in result[i]:
        val=val.decode()
        if val in nodeset:
            nodeset_exist.add(val)
            links.append(link(strlist[i], val))

for val in nodeset:
    if val in nodeset_exist:
        nodes.append(node(val))


g=Graph(ty,nodes,links)
with open('ipv4.js', 'w') as f:
    json.dump(g,default=lambda obj:obj.__dict__, fp=f,indent=4)


v6node=[]
v6nodeset=set()
v6nodeset_exist=set()
pipe=rp.pipeline()
for val in strlist:
    pipe.sismember("node_all_v6",val)
result=pipe.execute()


for i in range(0,len(result)):
    if result[i]:
        v6node.append(strlist[i])
    else:
        print(strlist[i])


pipe=rp.pipeline()
for key in v6node :
    pipe.smembers('node_asn_v6_{:s}'.format(key))
    v6nodeset.add(key)
result=pipe.execute()

ty="force"
nodes=[]
links=[]


for i in range(0, len(result)):
    for val in result[i]:
        val=val.decode()
        if val in v6nodeset:
            v6nodeset_exist.add(val)
            links.append(link(v6node[i], val))

for val in v6nodeset:
    if val in v6nodeset_exist:
        nodes.append(node(val))


g=Graph(ty,nodes,links)
with open('ipv6.js', 'w') as f:
    json.dump(g,default=lambda obj:obj.__dict__, fp=f,indent=4)
