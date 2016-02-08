""" Store a protobufer to / from a persistent storage backend.  """

from common.log import *
from common.store import s3
import abc


def write_to_file(proto_buf, filename):
    f = open(filename, 'wb')
    f.write(proto_buf.SerializeToString())
    f.close()
    return


def read_from_file(filename, proto_buf):
    f = open(filename, 'rb')
    proto_buf.ParseFromString(f.read())
    return proto_buf


class StorageError(Exception):

    """Base class for exceptions in this module."""
    pass


class ProtoStoreBase(object):
    __metaclass__ = abc.ABCMeta
    """ 
  Interface for protobuffer storage. 
  """
    @abc.abstractmethod
    def get(self, key):
        return NotImplemented

    @abc.abstractmethod
    def put(self, key, proto):
        return NotImplemented

    @abc.abstractmethod
    def __repr__(self):
        return NotImplemented


class S3ProtoStore(ProtoStoreBase):

    """ Stores protobuffer data in S3. """

    def __init__(self, bucket_name, protobuffer_class):
        self._protobuffer_class = protobuffer_class
        self._bucket_name = bucket_name
        self._s3_dict = s3.S3Dictionary(bucket_name)
        return

    def get(self, key):
        """ Return proto or None if not found. """
        proto = None
        data = self._s3_dict[key]
        if data:
            proto = self._protobuffer_class()
            proto.ParseFromString(data)
        return proto

    def put(self, key, proto):
        """ Store proto with key. """
        CHECK_EQ(proto.__class__, self._protobuffer_class)
        self._s3_dict[key] = proto.SerializeToString()
        return
    
    def delete(self, key):
        self._s3_dict.bucket.delete_key(key)
        return

    def __repr__(self):
        return '<S3ProtoStore bucket: %s>' % (self._bucket_name)


class FileProtoStore(ProtoStoreBase):

    def __init__(self, protobuffer_class):
        self._protobuffer_class = protobuffer_class
        return

    def get(self, filename):
        """ Return proto or None if filename not found. """
        proto = self._protobuffer_class()
        try:
            read_from_file(filename, proto)
        except IOError:
            proto = None
        return proto

    def put(self, filename, proto):
        """ Store proto in filename. """
        CHECK_EQ(proto.__class__, self._protobuffer_class)
        write_to_file(proto, filename)
        return

    def __repr__(self):
        return '<FileProtoStore %s>' % (self._protobuffer_class)
