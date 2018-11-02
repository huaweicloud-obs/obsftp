#!/usr/bin/python
# -*- coding:utf-8 -*-

import os
import json
import threading
import multiprocessing
import sys
import traceback
import functools
import operator
from .ilog import INFO, ERROR
from .const import LONG, IS_PYTHON2, UNICODE
from .model import BaseModel, CompletePart, CompleteMultipartUploadRequest, GetObjectRequest
from .util import safe_trans_to_gb2312, md5_file_encode_by_size_offset, to_long, to_int, to_string

if IS_PYTHON2:
    import Queue as queue
else:
    import queue


def _resumer_upload(bucketName, objectKey, uploadFile, partSize, taskNum, enableCheckPoint, checkPointFile, checkSum, metadata, obsClient):
    upload_operation = uploadOperation(to_string(bucketName), to_string(objectKey), to_string(uploadFile), partSize, taskNum, enableCheckPoint,
                                       to_string(checkPointFile), checkSum, metadata, obsClient)
    return upload_operation._upload()


def _resumer_download(bucketName, objectKey, downloadFile, partSize, taskNum, enableCheckPoint, checkPointFile,
                      header, versionId, obsClient):
    down_operation = downloadOperation(to_string(bucketName), to_string(objectKey), to_string(downloadFile), partSize, taskNum, enableCheckPoint, to_string(checkPointFile),
                                       header, versionId, obsClient)
    if down_operation.size == 0:
        with open(down_operation.fileName, 'wb') as _:
            pass
        return down_operation._metedata_resp
    return down_operation._download()


class Operation(object):
    def __init__(self, bucketName, objectKey, fileName, partSize, taskNum, enableCheckPoint, checkPointFile, obsClient):
        self.bucketName = bucketName
        self.objectKey = objectKey
        self.fileName = fileName
        self.partSize = partSize
        self.taskNum = taskNum
        self.enableCheckPoint = enableCheckPoint
        self.checkPointFile = checkPointFile
        self.obsClient = obsClient

    def _get_record(self):
        self.obsClient.log_client.log(INFO, 'load record file...')
        if not os.path.exists(self.checkPointFile):
            return None
        try:
            with open(safe_trans_to_gb2312(self.checkPointFile), 'r') as f:
                content = json.load(f)
        except ValueError:
            return None
        else:
            return _parse_string(content)

    def _delete_record(self):
        if os.path.exists(safe_trans_to_gb2312(self.checkPointFile)):
            os.remove(safe_trans_to_gb2312(self.checkPointFile))
            self.obsClient.log_client.log(INFO, 'del record file success. path is:{0}'.format(self.checkPointFile))

    def _write_record(self, record):
        with open(_to_unicode(self.checkPointFile), 'w') as f:
            json.dump(record, f)
            self.obsClient.log_client.log(INFO, 'write record file success. file path is {0}'.format(self.checkPointFile))


class uploadOperation(Operation):
    def __init__(self, bucketName, objectKey, uploadFile, partSize, taskNum, enableCheckPoint, checkPointFile,
                 checkSum, metadata, obsClient):
        super(uploadOperation, self).__init__(bucketName, objectKey, uploadFile, partSize, taskNum, enableCheckPoint,
                                              checkPointFile, obsClient)
        self.checkSum = checkSum
        self.metadata = metadata

        try:
            self.size = os.path.getsize(self.fileName)
            self.lastModified = os.path.getmtime(self.fileName)
        except Exception:
            self.obsClient.log_client.log(ERROR, 'something is happened when obtain uploadFile information. Please check')
            self._delete_record()
            raise Exception('something is happened when obtain uploadFile information. Please check')
        resp_for_check_bucket = self.obsClient.headBucket(self.bucketName)
        if resp_for_check_bucket.status > 300:
            raise Exception('touch  bucket {0} failed. Please check. Status:{1}.'.format(self.bucketName, resp_for_check_bucket.status))
        self._lock = threading.Lock()

        self._exception = []
        self._record = None

    def _upload(self):
        if not self.enableCheckPoint:
            self._prepare()
        else:
            self._load()
        self.__upload_parts = self._get_upload_parts()
        self._uploadinfos = []
        self._status = True
        thread_pools = _ThreadPool(functools.partial(self._produce, upload_parts=self.__upload_parts),
                                   [self._consume] * self.taskNum, self._lock)
        thread_pools.run()
        if not min(self._uploadinfos):
            if not self._status:
                self.obsClient.abortMultipartUpload(self.bucketName, self.objectKey, self._record['uploadId'])
                self.obsClient.log_client.log(ERROR, 'the code from server is 4**, please check space、persimission and so on.')
                self._delete_record()
                if len(self._exception) > 0:
                    raise Exception(self._exception[0])
            raise Exception('some parts are failed when upload. Please try agagin')
        part_Etags = []
        for part in sorted(self._record['partEtags'], key=lambda x: x['partNum']):
            part_Etags.append(CompletePart(partNum=part['partNum'], etag=part['etag']))
            self.obsClient.log_client.log(INFO, 'Completing to upload multiparts')
        resp = self.obsClient.completeMultipartUpload(self.bucketName, self.objectKey, self._record['uploadId'],
                                                      CompleteMultipartUploadRequest(part_Etags))
        if resp.status < 300:
            if self.enableCheckPoint:
                self._delete_record()
        else:
            if not self.enableCheckPoint:
                self.obsClient.abortMultipartUpload(self.bucketName, self.objectKey, self._record['uploadId'])
                self.obsClient.log_client.log(ERROR, 'something is wrong when complete multipart.ErrorCode:{0}. ErrorMessage:{1}'.format(
                    resp.errorCode, resp.errorMessage))
            else:
                if resp.status > 300 and resp.status < 500:
                    self.obsClient.abortMultipartUpload(self.bucketName, self.objectKey, self._record['uploadId'])
                    self.obsClient.log_client.log(ERROR, 'something is wrong when complete multipart.ErrorCode:{0}. ErrorMessage:{1}'.format(
                        resp.errorCode, resp.errorMessage))
                    self._delete_record()
        return resp

    def _load(self):
        self._record = self._get_record()
        if self._record and not (self._type_check(self._record) and self._check_upload_record(self._record)):
            if self._record['uploadId'] is not None:
                self.obsClient.abortMultipartUpload(self.bucketName, self.objectKey, self._record['uploadId'])
            self.obsClient.log_client.log(ERROR, 'checkpointFile is invalid')
            self._delete_record()
            self._record = None
        if not self._record:
            self._prepare()

    def _type_check(self, record):
        try:
            for key in ('bucketName', 'objectKey', 'uploadId', 'uploadFile'):
                if not isinstance(record[key], str):
                    self.obsClient.log_client.log(ERROR, '{0} is not a string type. {1} belong to {2}'.format(key, record[key],
                                                                                                              record[key].__class__))
                    return False
            if not isinstance(record['fileStatus'], list):
                self.obsClient.log_client.log(ERROR, 'fileStatus is not a list.It is {0} type'.format(record['fileStatus'].__class__))
                return False
            if not isinstance(record['uploadParts'], list):
                self.obsClient.log_client.log(ERROR, 'uploadParts is not a list.It is {0} type'.format(record['uploadParts'].__class__))
                return False
            if not isinstance(record['partEtags'], list):
                self.obsClient.log_client.log(ERROR, 'partEtags is not a dict.It is {0} type'.format(record['partEtags'].__class__))
                return False
        except KeyError as e:
            self.obsClient.log_client.log(INFO, 'Key is not found:{0}'.format(e.args))
            return False
        return True

    def _check_upload_record(self, record):
        if not ((record['bucketName'] == self.bucketName) and (record['objectKey'] == self.objectKey) and (record['uploadFile'] == self.fileName)):
            self.obsClient.log_client.log(INFO, 'the bucketName or objectKey or uploadFile was changed. clear the record')
            return False
        if record['uploadId'] is None:
            self.obsClient.log_client.log(INFO, '{0} (uploadId) not exist, clear the record.'.format(record['upload_id']))
            return False

        if record['fileStatus'][0] != self.size:
            self.obsClient.log_client.log(INFO, '{0} was changed, clear the record.'.format(self.fileName))
            return False
        if self.checkSum:
            checkSum = md5_file_encode_by_size_offset(file_path=self.fileName, size=self.size, offset=0)
            if record['fileStatus'][2] != checkSum:
                self.obsClient.log_client.log(INFO, '{0} content was changed, clear the record.'.format(self.fileName))
                return False
        return True

    def _file_status(self):
        fileStatus = []
        fileStatus.append(self.size)
        fileStatus.append(self.lastModified)
        if self.checkSum:
            fileStatus.append(md5_file_encode_by_size_offset(self.fileName, self.size, 0))
        return fileStatus

    def _slice_file(self):
        uploadParts = []
        num_counts = int(self.size/self.partSize)
        if num_counts >= 10000:
            import math
            self.partSize = int(math.ceil(float(self.size) / (10000-1)))
            num_counts = int(self.size / self.partSize)
        if self.size % self.partSize != 0:
            num_counts += 1
        offset = 0
        for i in range(1, num_counts+1, 1):
            length = to_long(self.partSize) if i != num_counts else to_long(self.size)
            part = Part(to_long(i), to_long(offset), length, False)
            offset += self.partSize
            uploadParts.append(part)
        return uploadParts

    def _get_upload_parts(self):
        final_upload_parts = []
        for p in self._record['uploadParts']:
            if not p['isCompleted']:
                final_upload_parts.append(p)
        return final_upload_parts

    def _prepare(self):
        fileStatus = self._file_status()
        uploadParts = self._slice_file()
        self.partETags = []
        resp = self.obsClient.initiateMultipartUpload(self.bucketName, self.objectKey, metadata=self.metadata)
        if resp.status > 300:
            raise Exception('initiateMultipartUpload failed. ErrorCode:{0}. ErrorMessage:{1}'.format(resp.errorCode, resp.errorMessage))
        self.uploadId = resp.body.uploadId
        self._record = {'bucketName': self.bucketName, 'objectKey': self.objectKey, 'uploadId': self.uploadId,
                        'uploadFile': self.fileName, 'fileStatus': fileStatus, 'uploadParts': uploadParts,
                        'partEtags': self.partETags}
        self.obsClient.log_client.log(INFO, 'prepare new upload task success. uploadId = {0}'.format(self.uploadId))
        if self.enableCheckPoint:
            self._write_record(self._record)

    def _produce(self, ThreadPool, upload_parts):
        for part in upload_parts:
            ThreadPool.put(part)

    def _consume(self, ThreadPool):
        while True:
            part = ThreadPool.get()
            if part is None:
                break
            self._upload_part(part)

    def _change_status(self):
        self._lock.acquire()
        try:
            self._status = False
        finally:
            self._lock.release()

    def _upload_part(self, part):
        if self._status:
            try:
                resp = self.obsClient.uploadPart(self.bucketName, self.objectKey, part['partNumber'], self._record['uploadId'], self.fileName,
                                                 isFile=True, partSize=part['length'], offset=part['offset'])
            except IOError:
                raise IOError('can not attach file {0}. Please check'.format(self.fileName))
            if resp.status < 300:
                complete_part = CompletePart(to_int(part['partNumber']), resp.body.etag)
                with self._lock:
                    self._record['uploadParts'][part['partNumber']-1]['isCompleted'] = True
                    self._record['partEtags'].append(complete_part)
                    self._uploadinfos.append(True)
                    if self.enableCheckPoint:
                        self._write_record(self._record)
            elif resp.status > 300 and resp.status < 500:
                self.obsClient.log_client.log(ERROR, 'response from server is something wrong. ErrorCode:{0}, ErrorMessage:{1}'.format(resp.errorCode, resp.errorMessage))
                self._exception.append('errorCode:{0}.errorMessage:{1}'.format(resp.errorCode, resp.errorMessage))
                self._change_status()
                self._uploadinfos.append(False)
        else:
            self._uploadinfos.append(False)


class downloadOperation(Operation):
    def __init__(self, bucketName, objectKey, downloadFile, partSize, taskNum, enableCheckPoint, checkPointFile,
                 header, versionId, obsClient):
        super(downloadOperation, self).__init__(bucketName, objectKey, downloadFile, partSize, taskNum, enableCheckPoint,
                                                checkPointFile, obsClient)
        self.header = header
        self.versionId = versionId
        self.obsClient = obsClient
        
        parent_dir = os.path.dirname(self.fileName)
        if not os.path.exists(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)
        
        metedata_resp = self.obsClient.getObjectMetadata(self.bucketName, self.objectKey, self.versionId)
        if metedata_resp.status < 300:
            self.lastModified = metedata_resp.body.lastModified
            self.size = metedata_resp.body.contentLength
        else:
            self.obsClient.log_client.log(ERROR, 'there are something wrong when touch the objetc {0}. ErrorCode:{1}, ErrorMessage:{2}'.format(self.objectKey, metedata_resp.errorCode, metedata_resp.errorMessage))
            self._delete_record()
            if os.path.exists(self.fileName+'.tmp'):
                os.remove(self.fileName+'.tmp')
            raise Exception('there are something wrong when touch the objetc {0}. ErrorCode:{1}, ErrorMessage:{2}'.format(self.objectKey, metedata_resp.status, metedata_resp.errorMessage))
        self._metedata_resp = metedata_resp
        self._lock = threading.Lock()
        self._tmp_file = self.fileName+'.tmp'
        self._record = None
        self._exception = []

    def _download(self):
        if not self.enableCheckPoint:
            self._prepare()
        else:
            self._load()
        self.__down_parts = self._get_down_part()
        self._downinfos = []
        self._status = True
        for part_info in self._record['downloadParts']:
            self._downinfos.append(part_info['isCompleted'])
        thread_pools = _ThreadPool(functools.partial(self._produce, download_parts=self.__down_parts), [self._consume] * self.taskNum, self._lock)
        thread_pools.run()
        if not min(self._downinfos):
            if not self._status:
                self._delete_record()
                if os.path.exists(self._tmp_file):
                    os.remove(self._tmp_file)
                if len(self._exception) > 0:
                    raise Exception(self._exception[0])
        try:
            os.rename(self._tmp_file, self.fileName)
            if self.enableCheckPoint:
                self._delete_record()
            self.obsClient.log_client.log(INFO, 'download success.')
            return self.obsClient.getObjectMetadata(self._record['bucketName'], self._record['objectKey'], self._record['versionId'])
        except OSError as e:
            if not self.enableCheckPoint:
                if os.path.exists(self._tmp_file):
                    os.remove(self._tmp_file)
            self.obsClient.log_client.log(INFO, 'Rename failed. The reason maybe:[the {0} exists, not a file path, not permission]. Please check.')
            raise e
        
    def _load(self):
        self._record = self._get_record()
        if self._record and not (self._type_record(self._record) and self._check_download_record(self._record)):
            self._delete_record()
            if os.path.exists(self._tmp_file):
                os.remove(self._tmp_file)
            self._record = None

        if not self._record:
            self._prepare()

    def _prepare(self):
        self.down_parts = self._split_object()
        object_staus = [self.objectKey, self.size, self.lastModified, self.versionId]
        with self._lock:
            with open(_to_unicode(self._tmp_file), 'w') as f:
                if self.size > 0:
                    f.seek(self.size-1, 0)
                if IS_PYTHON2:
                    f.write(b'b')
                else:
                    f.write(str('b'))
        tmp_file_status = [os.path.getsize(self._tmp_file), os.path.getmtime(self._tmp_file)]
        self._record = {'bucketName':self.bucketName, 'objectKey':self.objectKey, 'versionId':self.versionId,
                        'downloadFile':self.fileName, 'downloadParts':self.down_parts, 'objectStatus':object_staus,
                        'tmpFileStatus':tmp_file_status}
        self.obsClient.log_client.log(INFO, 'prepare new download task success.')
        if self.enableCheckPoint:
            self._write_record(self._record)

    def _type_record(self, record):
        try:
            for key in ('bucketName', 'objectKey', 'versionId', 'downloadFile'):
                if key == 'versionId' and record['versionId'] is None:
                    continue
                if not isinstance(record[key], str):
                    self.obsClient.log_client.log(ERROR, '{0} is not a string type. {1} belong to {2}'.format(key, record[key],
                                                                                                              record[key].__class__))
                    return False
            if not isinstance(record['downloadParts'], list):
                self.obsClient.log_client.log(ERROR, 'downloadParts is not a list.It is {0} type'.format(record['downloadParts'].__class__))
                return False
            if not isinstance(record['objectStatus'], list):
                self.obsClient.log_client.log(ERROR, 'objectStatus is not a list.It is {0} type'.format(record['objectStatus'].__class__))
                return False
            if not isinstance(record['tmpFileStatus'], list):
                self.obsClient.log_client.log(ERROR, 'tmpFileStatus is not a dict.It is {0} type'.format(record['tmpFileStatus'].__class__))
                return False
        except KeyError as e:
            self.obsClient.log_client.log(INFO, 'Key is not found:{0}'.format(e.args))
            return False
        return True

    def _check_download_record(self, record):
        if not operator.eq([record['bucketName'], record['objectKey'], record['versionId'], record['downloadFile']],
                           [self.bucketName, self.objectKey, self.versionId, self.fileName]):
            return False
        object_meta_resp = self.obsClient.getObjectMetadata(self.bucketName, self.objectKey, self.versionId)
        if object_meta_resp.status < 300:
            if not operator.eq(record['objectStatus'], [self.objectKey, object_meta_resp.body.contentLength,
                                                        object_meta_resp.body.lastModified, self.versionId]):
                return False
        elif object_meta_resp > 300 and object_meta_resp < 500:
            self._delete_record()
            if os.path.exists(self._tmp_file):
                os.remove(self._tmp_file)
            raise Exception('there are something wrong when touch the objetc {0}. ErrorCode:{1}, ErrorMessage:{2}'.format(self.objectKey, object_meta_resp.errorCode, object_meta_resp.errorMessage))
        else:
            return False
        if not operator.eq(record['tmpFileStatus'], [os.path.getsize(self._tmp_file), os.path.getmtime(self._tmp_file)]):
            return False
        return True

    def _get_down_part(self):
        down_parts = []
        for part in self._record['downloadParts']:
            if not part['isCompleted']:
                down_parts.append(part)
        return down_parts

    def _split_object(self):
        downloadParts = []
        num_counts = int(self.size / self.partSize)
        if num_counts >= 10000:
            import math
            self.partSize = int(math.ceil(float(self.size) / (10000 - 1)))
            num_counts = int(self.size / self.partSize)
        if self.size % self.partSize != 0:
            num_counts += 1
        offset = 0
        for i in range(1, num_counts + 1, 1):
            length = to_long(self.partSize) if i != num_counts else to_long(self.size)
            part = Part(to_long(i), to_long(offset), length, False)
            offset += self.partSize
            downloadParts.append(part)
        return downloadParts

    def _produce(self, ThreadPool, download_parts):
        for part in download_parts:
            ThreadPool.put(part)

    def _consume(self, ThreadPool):
        while ThreadPool.OK():
            part = ThreadPool.get()
            if part is None:
                break
            self._download_part(part)

    def _change_status(self):
        self._lock.acquire()
        try:
            self._status = False
        finally:
            self._lock.release()

    def _download_part(self, part):
        get_object_request = GetObjectRequest(versionId=self.versionId)
        self.header.range = str(part['offset'])+'-'+str(part['offset']+part['length'])
        try:
            resp = self.obsClient.getObject(bucketName=self.bucketName, objectKey=self.objectKey, getObjectRequest=get_object_request, headers=self.header)
            if resp.status < 300:
                respone = resp.body.response
                chunk_size = 65536
                with self._lock:
                    if respone is not None:
                        with open(_to_unicode(self._tmp_file), 'rb+') as fs:
                            fs.seek(part['offset'], 0)
                            position = to_int(part['offset'])
                            while True:
                                chunk = respone.read(chunk_size)
                                if not chunk:
                                    break
                                fs.write(chunk)
                                position += chunk_size
                                fs.seek(position, 0)
                            fs.close()
                        respone.close()
                    self._downinfos.append(True)
                    self._record['downloadParts'][part['partNumber']-1]['isCompleted'] = True
            elif resp.status > 300 and resp.status < 500:
                with self._lock:
                    self._downinfos.append(False)
                    self._change_status()
                    self._exception.append('response from server is something wrong. ErrorCode:{0}, ErrorMessage:{1}'
                                           .format(resp.errorCode, resp.errorMessage))
                    self.obsClient.log_client.log(ERROR, 'response from server is something wrong. ErrorCode:{0}, ErrorMessage:{1}'
                                                  .format(resp.errorCode, resp.errorMessage))
            else:
                self._downinfos.append(False)
                self._exception.append('response from server is something wrong. ErrorCode:{0}, ErrorMessage:{1}'
                                       .format(resp.errorCode, resp.errorMessage))
        except Exception as e:
            self.obsClient.log_client.log(ERROR, 'something wraong happened. Please check.')
            raise e
        finally:
            if self.enableCheckPoint:
                with self._lock:
                    self._record['tmpFileStatus'][1] = os.path.getmtime(self._tmp_file)
                    self._write_record(self._record)


class Part(BaseModel):

    allowedAttr = {'partNumber': LONG, 'offset' : LONG, 'length' : LONG, 'isCompleted': bool}

    def __init__(self, partNumber, offset, length, isCompleted=False):
        self.partNumber = partNumber
        self.offset = offset
        self.length = length
        self.isCompleted = isCompleted


def _parse_string(content):
    if IS_PYTHON2:
        if isinstance(content, dict):
            return dict([(_parse_string(key), _parse_string(value)) for key, value in content.iteritems()])
        elif isinstance(content, list):
            return [_parse_string(element) for element in content]
        elif isinstance(content, UNICODE):
            return content.encode('utf-8')
    return content


def _to_unicode(data):
    if isinstance(data, bytes):
        return data.decode('utf-8')
    return data


class _ThreadPool(object):
    def __init__(self, producer, consumers, lock):
        self._producer = producer
        self._consumers = consumers
        self._lock = lock
        self._queue = queue.Queue()

        self._threads_consumer = []
        self._threads_producer = []
        self._threading_thread = threading.Thread
        self._exc_info = None
        self._exc_stack = None

    def run(self):
        self._add_and_run(self._threading_thread(target=self._producer_start), self._threads_producer)
        for thread in self._threads_producer:
            thread.join()
        for consumer in self._consumers:
            self._add_and_run(self._threading_thread(target=self._consumer_start, args=(consumer,)), self._threads_consumer)

        for thread in self._threads_consumer:
            thread.join()

        if self._exc_info:
            raise self._exc_info[1]

    def put(self, task):
        assert task is not None
        self._queue.put(task)

    def get(self):
        return self._queue.get()

    def OK(self):
        with self._lock:
            return self._exc_info is None

    def _add_and_run(self, thread, pool):
        thread.daemon = True
        thread.start()
        pool.append(thread)

    def _producer_start(self):
        try:
            self._producer(self)
        except Exception:
            with self._lock:
                if self._exc_info is None:
                    self._exc_info = sys.exc_info()
                    self._exc_stack = traceback.format_exc()
            self._put_end()
        else:
            self._put_end()

    def _consumer_start(self, consumer):
        try:
            consumer(self)
        except Exception:
            with self._lock:
                if self._exc_info is None:
                    self._exc_info = sys.exc_info()
                    self._exc_stack = traceback.format_exc()

    def _put_end(self):
        for _ in range(len(self._consumers)):
            self._queue.put(None)

