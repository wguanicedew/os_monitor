import json

objects = {}
file = open("/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_ddmendpoints_objectstores.json")
objectstores = json.load(file)
#print objectstores
for obj in objectstores:
    print obj
    #print objectstores[obj]['rprotocols']
    for proId in objectstores[obj]['rprotocols']:
        print objectstores[obj]['rprotocols'][proId]
        if 'activities' in objectstores[obj]['rprotocols'][proId] and 'w' in objectstores[obj]['rprotocols'][proId]['activities']:
            print objectstores[obj]['rprotocols'][proId]['endpoint']
            objects[obj] = {'endpoint': objectstores[obj]['rprotocols'][proId]['endpoint'], 'is_secure': objectstores[obj]['rprotocols'][proId]['settings']['is_secure']}

print objects
