# -*- coding: utf-8 -*-

import obs
import os,sys,time,datetime
import logging


class _ListAll(object):
    def __init__(self,  marker=''):
        self.marker =  marker
        self.is_Truncated = True
        self.entity = []

    def _listresult(self):
        raise NotImplemented
    def __iter__(self):
        return self
    def next(self):
        while True:
            if self.entity:
                return self.entity.pop(0)
            if not self.is_Truncated:
                raise StopIteration()
            self._listresult()

class ListAllObjects(_ListAll):
    def __init__(self,service,bucket_name, prefix='', marker='', delimiter='', max_keys=1000):
        super(ListAllObjects,self).__init__(marker)
        self.service = service
        self.bucket_name = bucket_name
        self.prefix  = prefix
        self.marker = marker
        self.delimiter = delimiter
        self.max_keys = max_keys
    def _listresult(self):
        logger = logging.getLogger('pyftpdlib')
        result = self.service.listObjects(self.bucket_name, prefix=self.prefix,marker=self.marker, max_keys=self.max_keys, delimiter=self.delimiter)
        logger.info("listresult %r" % result.body)
        for content in result.body.contents:
            self.entity.append(content)
        for dir in result.body.commonPrefixs:
            self.entity.append(EnhanceObjectInfo(dir.prefix, None, None, None, None))
        self.entity.sort(key=lambda obj : obj.key)
        self.is_Truncated = result.body.is_truncated
        self.marker = result.body.next_marker

# This class is used when there is no property when getting a directory
# or when you need to customize some properties of the object.
class EnhanceObjectInfo(object):
    def __init__(self, key, lastModified, etag, size, storageClass):
        #: object or dir name。
        self.key = key
        #: defined the last modified time of object
        self.lastModified = lastModified
        #: HTTP ETag
        self.etag = etag
        #: object contentlength
        self.size = size
        #: object.storageclass , usual to be STANDARD,WARM,COLD
        self.storageClass = storageClass

    def is_prefix(self):
        # IF the obsject is a dir ,judge lastModefied is None
        return self.lastModified is None

class RequestResult(object):
    def __init__(self, resp):
        #: HTTP response pointer
        self.resp = resp
        #: HTTP response code
        self.status = resp.status
        #: HTTP response header
        self.headers = resp.headers
        #: OBS Only ID of every request, if have some request error,like 5xx,you can provide this id to cloud tecchnical support.
        self.request_id = resp.requestId


