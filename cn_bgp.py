import  json
import redis

strlist=[]
with open('CN-20201107.txt', 'r') as f:
    for i in range(500):
        str=f.readline().replace('\n',"").replace("AS","")
        strlist.append(str)

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

node_prefix = 'node_asn_{:s}'

pipe=rp.pipeline()
for key in strlist :
    pipe.smembers(node_prefix.format(key))
result=pipe.execute()

ty="force"
nodeset=set(strlist)
nodes=[]
links=[]
nodeset_exist=set()
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
with open('cn.js', 'w') as f:
    json.dump(g,default=lambda obj:obj.__dict__, fp=f,indent=4)


