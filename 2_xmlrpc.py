import xmlrpc.client
with xmlrpc.client.ServerProxy("http://cse224.sysnet.ucsd.edu:7777/RPC2") as proxy:
    print(proxy.litserver.getLiterature("A53318133"))

"""
keyword: 220
literature:
The wren
Earns his living
Noiselessly.
- Kobayahsi Issa
"""
