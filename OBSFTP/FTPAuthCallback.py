# Copyright (C) 2018 charles    nahra@163.com
# Use of this source code is governed by MIT license
# -*- coding: utf-8 -*-
import os, sys

current_path = os.path.dirname(os.path.abspath(__file__))
root_path = os.path.abspath( os.path.join(current_path, os.pardir))
if sys.platform.startswith("linux"):
    python_lib_path = os.path.abspath( os.path.join(root_path, "python27", "unix", "lib"))
    sys.path.append(python_lib_path)
elif sys.platform == "darwin":
    python_lib_path = os.path.abspath( os.path.join(root_path, "python27", "unix", "lib"))
    sys.path.append(python_lib_path)
    extra_lib = "/System/Library/Frameworks/Python.framework/Versions/2.7/Extras/lib/python/PyObjc"
    sys.path.append(extra_lib)
elif sys.platform == "win32":
    pass
else:
    raise RuntimeError("detect platform fail:%s" % sys.platform)

import time
import logging

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.authorizers import AuthenticationFailed
from pyftpdlib.authorizers import AuthorizerError
import obsadapter
import Constants


class BucketLoginInfo():
    def __init__(self, bucket_name, access_key_id, access_key_secret, endpoint):
        self.bucket_name = bucket_name
        self.endpoint = endpoint
        self.access_key = {access_key_id:access_key_secret}
        self.expire_time = time.time() + 60

    def update_access_key(self, access_key_id, access_key_secret):
        self.access_key[access_key_id] = access_key_secret
        self.expire_time = time.time() + 60

    def expired(self):
        return self.expire_time < time.time()

class ObsAuthorizer(DummyAuthorizer):

    default_endpoint = Constants.transmode + "obs.myhwclouds.com"
    LOCAL_CHECK_OK = 0
    LOCAL_CHECK_FAIL = 1
    LOCAL_CHECK_UNCERTAIN = 2
    
    def __init__(self):
        self.bucket_info_table = {}
        self.expire_time_interval = 60
        self.internal = None
        self.bucket_endpoints = {}

    def parse_username(self, username):
        if len(username) == 0:
            raise AuthorizerError("username can't be empty!")
        index = username.rfind('/')
        if index == -1:
            raise AuthorizerError("username %s is not in right format, it should be like ACCESSKEY_ID/BUCKET_NAME" % username)
        elif index == len(username) - 1:
            raise AuthorizerError("bucketname can't be empty!")
        elif index == 0:
            raise AuthorizerError("ACCESSKEY_ID can't be empty!")

        return  username[index+1:], username[:index]
    
    def log_bucket_info(self, bucket_name, endpoint, access_key_id):
        work_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
        file_name = work_dir + '/log/obsftp/obsftp.info'
        logger = logging.getLogger('pyftpdlib')
        try:
            f = open(file_name, 'a')
            time_str = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
            record = "%s\tBucket:%s\tEndpoint:%s\tAccessID:%s\n" % (time_str, bucket_name, endpoint, access_key_id)
            f.write(record)
            f.close()
            logger.info("recording bucket access info succeed.%s" % record)
        except IOError as err:
            logger.error("error recording bucket access info.%s" % unicode(err))
    # obsftp.info 中写入信息
    def put_bucket_info(self, bucket_name, endpoint, access_key_id, access_key_secret):
        if bucket_name not in self.bucket_info_table:
            self.bucket_info_table[bucket_name] = BucketLoginInfo(bucket_name, 
                    access_key_id, access_key_secret, endpoint)
        else:
            bucket_info = self.get_bucket_info(bucket_name)
            bucket_info.update_access_key(access_key_id, access_key_secret)
        self.log_bucket_info(bucket_name, endpoint, access_key_id)

    def get_bucket_info(self, bucket_name):
        return self.bucket_info_table.get(bucket_name)

    def delete_bucket_info(self, bucket_name):
        self.bucket_info_table.pop(bucket_name, None)


    def obs_bucket_auth(self, bucket_name, default_endpoint, access_key_id, access_key_secret):
        logger = logging.getLogger('pyftpdlib')
        try:
            service = obsadapter.ObsClient(access_key_id=access_key_id, secret_access_key=access_key_secret, server=default_endpoint)
            res_location = service.getBucketLocation(bucket_name)
            logger.info("location message is %s" % res_location.body.location)

        except service.status is 403:
            raise AuthenticationFailed("can't Get bucket Location, check your access_key.request_id:%s, status:%s, code:%s, message:%s"% (service.requestId, unicode(service.status), service.errorCode, service.errorMessage))
        except res_location.status >= 300:
            raise AuthenticationFailed("get buckets Location error. request_id:%s, status:%s, code:%s, message:%s" % (res_location.requestId, unicode(res_location.status), res_location.errorCode, res_location.errorMessage))
        
        #  get the bucket Region endpoint
        if res_location.body.location !='' :
           endpoint =  Constants.transmode +"obs." + res_location.body.location + ".myhwclouds.com"
           logger.info("the endpoint is %s" % endpoint)
           return endpoint
        else:
           raise AuthenticationFailed("can't find the OBS bucket's endpoint %s when query buckets in server." % bucket_name)

    def local_check(self, bucket_name, access_key_id, access_key_secret):
        bucket_info = self.get_bucket_info(bucket_name)
        if bucket_info is None:
            return self.LOCAL_CHECK_UNCERTAIN 
        if bucket_info.expired():
            self.delete_bucket_info(bucket_name)
            return self.LOCAL_CHECK_UNCERTAIN
        if access_key_id not in bucket_info.access_key:
            return self.LOCAL_CHECK_UNCERTAIN
        if bucket_info.access_key[access_key_id] != access_key_secret:
            raise AuthenticationFailed("AuthFailed, bucket:%s, access_key_id:%s, access_key_secret is not right" % (bucket_name, access_key_id))
        else:
            return self.LOCAL_CHECK_OK

    def validate_authentication(self, username, password, handler):
        """Raises AuthenticationFailed if supplied username and
        password don't match the stored credentials, else return
        None.
        """
        bucket_name, access_key_id = self.parse_username(username)
        access_key_secret = password
        res = self.local_check(bucket_name, access_key_id, access_key_secret)
        if res == self.LOCAL_CHECK_OK:
            return
        endpoint = self.obs_bucket_auth(bucket_name, self.default_endpoint, access_key_id, access_key_secret)
        self.put_bucket_info(bucket_name, endpoint, access_key_id, access_key_secret)

    def get_home_dir(self, username):
        """Return the user's home directory.
        Since this is called during authentication (PASS),
        AuthenticationFailed can be freely raised by subclasses in case
        the provided username no longer exists.
        """
        bucket_name, access_key_id = self.parse_username(username)
        bucket_name = bucket_name.strip('/')
        bucket_name = '/' + bucket_name + '/'
        return bucket_name 

    def impersonate_user(self, username, password):
        """Impersonate another user (noop).

        It is always called before accessing the filesystem.
        By default it does nothing.  The subclass overriding this
        method is expected to provide a mechanism to change the
        current user.
        """

    def terminate_impersonation(self, username):
        """Terminate impersonation (noop).

        It is always called after having accessed the filesystem.
        By default it does nothing.  The subclass overriding this
        method is expected to provide a mechanism to switch back
        to the original user.
        """

    def has_perm(self, username, perm, path=None):
        """Whether the user has permission over path (an absolute
        pathname of a file or a directory).

        Expected perm argument is one of the following letters:
        "elradfmwM".
        """
        return perm in (self.write_perms + self.read_perms)

    def get_perms(self, username):
        """Return current user permissions."""
        return self.write_perms + self.read_perms

    def get_msg_login(self, username):
        """Return the user's login message."""
        bucket_name, access_key_id = self.parse_username(username)
        msg = u"login to bucket: %s with access_key_id: %s" % (bucket_name, access_key_id)
        return msg 

    def get_msg_quit(self, username):
        """Return the user's quitting message."""
        bucket_name, access_key_id = self.parse_username(username)
        msg = u"logout of bucket: %s with access_key_id: %s" % (bucket_name, access_key_id)
        return msg 
