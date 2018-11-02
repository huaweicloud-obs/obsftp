# -*- coding: utf-8 -*-
from pyftpdlib.filesystems import FilesystemError
import obsadapter
import ObjectOperation
import Constants

class FTPOpCallback:

    def __init__(self, bucket_name, endpoint, access_id, access_key):
        self.bucket_name = bucket_name
        self.endpoint = endpoint
        self.access_id = access_id
        self.access_key = access_key
        self.is_secure = False
        self.service =obsadapter.ObsClient(self.access_id, self.access_key, server=self.endpoint, is_secure=self.is_secure)
        self.size_cache = {}
        self.dir_cache = {}

    def is_bucket(self, path):
        phyPath = path.rstrip('/')
        index = phyPath.rfind('/')
        if index == 0 and not self.is_root(path):
            return True
        return False
    
    def is_root(self, path):
        return path == '/'

    def get_bucket_name(self, path):
        if self.is_root(path):
            return u'/'
        phyPath = path.rstrip('/')
        index = phyPath.find('/', 1)
        if index <= 0:
            return phyPath[1:]
        else:
            return phyPath[1:index]
        
    def get_file_name(self, path):
        if self.is_bucket(path):
            return ""
        if path == '/':
            return u'/'
        bucket = self.get_bucket_name(path)
        return path[len(bucket)+2:]
    
    def normalize_separate_char(self, path):
        normalized_path_name = path.replace('\\', '/')
        return normalized_path_name
    
    def get_service(self, path):
        path = self.normalize_separate_char(path)
        service = obsadapter.ObsClient(self.access_id, self.access_key, server=self.endpoint)
        return service
    
    def get_object(self, path):
        path = self.normalize_separate_char(path)
        object = self.get_file_name(path)
        return object

    def get_file_operation_instance(self, path):
        return ObjectOperation.TransFileToObject(self.get_service(path), self.bucket_name, self.get_object(path), self.size_cache, self.dir_cache)

    def open_read(self, path):
        return self.get_file_operation_instance(path).open_read()
    
    def open_write(self, path):
        return self.get_file_operation_instance(path)
    
    def mkdir(self, path):
        return self.get_file_operation_instance(path).mkdir()
   
    def infopath(self, path):
        return self.get_file_operation_instance(path).infopath()
    
    def listdir(self, path):
        return self.get_file_operation_instance(path).listdir()
    
    def rmdir(self, path):
        return self.get_file_operation_instance(path).rmdir()
    
    def remove(self, path):
        return self.get_file_operation_instance(path).remove()
    
    def rename(self, path1, path2):
        raise FilesystemError("rename is not support now")
    
    def getsize(self, path):
        return self.get_file_operation_instance(path).getsize()

    def getmtime(self, path):
        return self.get_file_operation_instance(path).getmtime()
    
    def isfile(self, path):
        return self.get_file_operation_instance(path).isfile()
    
    def isdir(self, path):
        path = self.normalize_separate_char(path)
        if self.is_bucket(path):
            return True
        if self.is_root(path):
            return True
        return self.get_file_operation_instance(path).isdir()
    def lexists(self, path):
        return self.isfile(path) or self.isdir(path)
