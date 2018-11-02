# -*- coding: utf-8 -*-

from obs import *

from ObjectOperationMore import *


AK = 'AFTLYZWWP3CZ5AJQSSSC'
SK = 'qMG3L1hTu0suQ4gNipYQTZoOdfZQ94UCoBvzazku'
server = 'http://obs.myhwclouds.com'

bucketName = 'obs-tools'


# Constructs a obs client instance with your account for accessing OBS
obsClient = ObsClient(access_key_id=AK, secret_access_key=SK, server=server)
lisresut = ListAllObjects(obsClient,bucketName,delimiter='/')
dir_list=[]
key_list=[]
for m, n in enumerate(lisresut):
    if n.lastModified is None:
        print("the dir is %r",n.key)
        dir_list.append(n.key)
    else:
        key_list.append(n)
        print("the object is:%r,modfiedtime:%r" %(n.key,n.lastModified))
