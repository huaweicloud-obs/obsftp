# Copyright (C) 2018 charles    nahra@163.com
# Use of this source code is governed by MIT license
# -*- coding: utf-8 -*-
import time

from pyftpdlib.filesystems import FilesystemError
import obsadapter
from obsadapter import *
import logging
import Constants



class PosixFile:
    def __init__(self, name, resp):
        self.name = name
        self.resp = resp
        self.closed = False

    def read(self, chunksize=None):
        return self.resp.body.response.read(chunksize)

    def close(self):
        pass


class TransFileToObject:

    #def __init__(self, bucket, key, size_cache, dir_cache):
    def __init__(self, service, bucket_name, key, size_cache, dir_cache):
        self.service = service
        self.bucket_name = bucket_name
        self.key = key.lstrip('/')
        self.size_cache = size_cache
        self.dir_cache = dir_cache
        self.expire_time = 10

        self.buf = ''
        self.buflimit = Constants.send_data_buff_size
        self.closed = False
        self.name = self.bucket_name + '/' + self.key
        self.upload_id = None
        self.part_num = None
        self.part_list = []
        self.contents = None


    def init_multi_upload(self):
        resp =self.service.initiateMultipartUpload(self.bucket_name, self.key)
        #resp = self.service.init_multipart_upload(self.key)
        self.upload_id = resp.body.uploadId
        self.part_num = 0
        self.part_list = []
        return self.upload_id

    def get_upload_id(self):
        if self.upload_id is not None:
            return self.upload_id
        else:
            return self.init_multi_upload()
    

    def upload_part(self):
        return self.service.uploadPart( self.bucket_name,self.key, self.part_num, self.upload_id, self.buf)

    def send_buf(self):
        upload_id = self.get_upload_id()
        assert upload_id is not None
        if not self.buf:
            return
        self.part_num += 1
        res = self.upload_part()
        self.buf = ''
        self.part_list.append(obsadapter.CompletePart(self.part_num, res.body.etag))

       
    def write(self, data):
        while len(data) + len(self.buf) > self.buflimit:
            _len = self.buflimit - len(self.buf)
            self.buf = self.buf + data[:_len]
            data = data[_len:]
            self.send_buf()
        self.buf += data
    

    def put_object(self, buf):
        self.service.putObject(self.bucket_name, self.key, buf)


    def complete_multipart_upload(self):
        completeMultipart = obsadapter.CompleteMultipartUploadRequest(self.part_list)
        self.service.completeMultipartUpload(self.bucket_name,self.key, self.upload_id, completeMultipart)

    def close(self):
        assert self.closed == False
        if self.upload_id is None:
            self.put_object(self.buf)
        else:
            self.send_buf()
            self.complete_multipart_upload()
        self.closed = True
    
    def listdir(self):
        logger = logging.getLogger('pyftpdlib')
        key = self.key
        if key != '':
            # 删除末尾‘/’
            key = key.rstrip('/') + '/'
        self.key_list = []
        self.dir_list = []
        list_res = obsadapter.ListAllObjects(self.service, self.bucket_name, prefix=key, delimiter='/')
        logger.info("recording bucket access info succeed.%r" % list_res)
        for i, key_info in enumerate(list_res):
            # 通过lastModified来判断是否是目录，因为有内容为空的文件所以不用size判断
            if  key_info.lastModified is None:
                self.dir_list.append(key_info.key)
            else:
                self.key_list.append(key_info)
        logger.info("recording object info is %r" % self.key_list)
        logger.info("recording dir info is .%r" % self.dir_list)
        self.contents = []
        for entry in self.key_list:
            to_add = entry.key.decode('utf-8')[len(key):]
            # last_modified = entry.lastModified
            last_modified_str = entry.lastModified
            logger.info("recording bucket access info succeed.%r" % entry.lastModified)
            # last_modified_str = datetime.datetime.utcfromtimestamp(last_modified).strftime('%Y/%m/%d %H:%M:%S')
            self.contents.append((to_add, entry.size, last_modified_str.decode('utf-8')))
            self.cache_set(self.size_cache, (self.bucket_name, entry.key), entry.size)
        for entry in self.dir_list:
            to_add = entry.decode('utf-8')[len(key):]
            self.contents.append((to_add, -1, 0))
        return self.contents
   
    # show info about path
    def infopath(self):
        size = -1
        last_modified_str = u"1970/01/01 00:00:00" 
        if self.object_exists():
            size, last_modified = self.info_object()
            last_modified_str =last_modified.decode('utf-8')
            # last_modified_str = datetime.datetime.utcfromtimestamp(last_modified).strftime('%Y/%m/%d %H:%M:%S').decode('utf-8')

        return size, last_modified_str 

    #
    # def object_exists(self):
    #     return self.bucket.object_exists(self.key)


    def object_exists(self):
        # return self.bucket.object_exists(self.key)
        # GMT_FORMAT = '%a, %d %b %Y %H:%M:%S GMT'
        # date = (datetime.datetime.utcnow() + datetime.timedelta(days=1)).strftime(GMT_FORMAT)
        # resp = self.service.getObject(self.bucket_name, self.key, headers={'if-modified-since': date})
        resp = self.service.getObjectMetadata(self.bucket_name, self.key)
        if resp.status < 300:
            return True
        else:
            return False

    def isfile(self):
        return self.object_exists()

    def isdir(self):
        value = self.cache_get(self.dir_cache, (self.bucket_name, self.key))
        if value is not None:
            return value
        contents = self.listdir()
        _is_dir = not  (len(contents) == 0)
        self.cache_set(self.dir_cache, (self.bucket_name, self.key), _is_dir)
        return _is_dir
    
    def cache_get(self, cache, key):
        if not cache.has_key(key):
            return None
        if cache[key][1] + self.expire_time >= time.time():
            return cache[key][0]
        else:
            self.cache_delete(cache, key)
            return None
    
    def cache_set(self, cache, key, value):
        cache[key] = (value, time.time())

    def cache_delete(self, cache, key):
        cache.pop(key, None)

    def info_object(self):
        resp = self.service.getObjectMetadata(self.bucket_name, self.key)
        content_length = resp.body.contentLength
        if not content_length:
            content_length = 0
        mtime = resp.body.lastModified
        return content_length, mtime


    def head_object(self):
        resp = self.service.getObjectMetadata(self.bucket_name, self.key)
        content_length = resp.body.contentLength
        return content_length


    def getmtime(self):
        resp = self.service.getObjectMetadata(self.bucket_name, self.key)
        mtime = resp.body.lastModified
        return mtime

    def getsize(self):
        value = self.cache_get(self.size_cache, (self.bucket_name, self.key))
        if value != None:
            return value
        content_length = self.head_object()
        self.cache_set(self.size_cache, (self.bucket_name, self.key), content_length)
        return content_length
 
    #
    def get_object(self):
        resp = self.service.getObject(self.bucket_name, self.key, loadStreamInMemory=False)
        return PosixFile(self.name, resp)

    def open_read(self):
        return self.get_object()
       
    def mkdir(self):
        self.key = self.key.rstrip('/') + '/'
        self.put_object('')
        self.key = self.key.rstrip('/')
        self.cache_set(self.dir_cache, (self.bucket_name, self.key), True)


    def delete_object(self):
        del_res = self.service.deleteObject(self.bucket_name,self.key)
        return RequestResult(del_res)
        # self.bucket.delete_object(self.key)

    def rmdir(self):
        self.key = self.key.rstrip('/') + '/'
        self.delete_object()
        self.key = self.key.rstrip('/')
        self.cache_set(self.dir_cache, (self.bucket_name, self.key), False)

    def remove(self):
        self.delete_object()
        self.cache_delete(self.size_cache, (self.bucket_name, self.key))

