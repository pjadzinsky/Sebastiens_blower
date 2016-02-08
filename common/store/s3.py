""" Interfaces to persistent storage backed by s3. 

Note: Defaults to reduce redundancy.
"""


from boto.s3 import key as s3_key
from common.log import *
import boto
import mimetypes
import glob

def get_bucket_location(bucket_name):
    conn = boto.connect_s3()
    bucket = None
    try:
        bucket = conn.get_bucket(bucket_name, validate=True)
    except boto.exception.S3ResponseError:
        pass
    CHECK_NOTNONE(bucket, 'Bucket does not exist: %s' % (bucket_name))
    bucket_location = bucket.get_location()
    if not bucket_location:  # strange, but no location means us-east-1
        return 'us-east-1'
    return bucket_location

def get_connection_in_bucket_region(bucket_name):
    conn = boto.s3.connect_to_region(get_bucket_location(bucket_name))
    return conn

bucket_cache = {}

class BaseInterface(object):

    def __init__(self, bucket_name, reduced_redundancy=True):
        global bucket_cache
        self._bucket_name = bucket_name
        self._reduced_redundancy = reduced_redundancy
        
        if bucket_name in bucket_cache:
            self.bucket = bucket_cache[bucket_name]
            #LOG(INFO, 'using cached bucket')
        else:
            bucket_location = get_bucket_location(bucket_name)
            # NOTE: Ensure we use a connection in same region as bucket
            # While s3 buckets can be accessed from any region, certain operations
            # on large files will fail unless you use a boto connection to the same
            # region that hosts the bucket.
            # See boto issue: https://github.com/boto/boto/issues/2207
            s3 = boto.s3.connect_to_region(bucket_location)
            LOG(INFO, 'Attempting to connect to bucket: %s' % (bucket_name))
            self.bucket = s3.lookup(bucket_name)
            CHECK_NOTNONE(self.bucket, 'Bucket does not exist: "%s"' %
                      (self._bucket_name))
            bucket_cache[bucket_name] = self.bucket
        
        return


class S3Dictionary(BaseInterface):

    """ Adapts an s3 bucket to give it the interface of a python dictionary.  """

    def __init__(self, bucket_name, reduced_redundancy=True):
        super(S3Dictionary, self).__init__(bucket_name, reduced_redundancy)
        
        return

    def get(self, key):
        """ Return value or None if not found. """
        value = None
        entry = self.bucket.get_key(key)
        if entry:
            value = entry.get_contents_as_string()
        return value
    
    def get_key(self, key):
        """ Return key directly.
        Useful for accessing metadata or other properties
        """
        return self.bucket.get_key(key)
        

    def set(self, key, value, headers={}, metadata={}):
        """ Store value with key. """
        entry = s3_key.Key(self.bucket)
        entry.key = key
        for k,v in metadata.iteritems():
            entry.set_metadata(k,v)
        entry.set_contents_from_string(value, headers=headers, reduced_redundancy = self._reduced_redundancy)
        return

    def __setitem__(self, key, value):
        return self.set(key, value)

    def __getitem__(self, key):
        return self.get(key)

    def __repr__(self):
        return '<S3Dictionary bucket: %s>' % (self._bucket_name)


class S3FileStore(BaseInterface):

    """ Interface to an s3 bucket that is file-oriented. """

    def __init__(self, bucket_name, reduced_redundancy=True):
        super(S3FileStore, self).__init__(bucket_name, reduced_redundancy)
        return

    def key_exists(self, key_str):
        """ Return True if key exists. 

        Args:
          key_str (str): the key to check for
        """
        key = self.bucket.get_key(key_str)
        exists = False
        if key:
            exists = True
        return exists
    
    def key_size(self, key_str):
        """ Return file size if bytes if key exists. 

        Args:
          key_str (str): the key to check for
        """
        key = self.bucket.get_key(key_str)
        size = None
        if key:
            size = key.size
        return size

    def key_set_exists(self, keys_list):
        """ Return True if all keys exist. 

        Args:
          keys_list (list of str): the keys to check for
        """
        all_exist = True
        for key_str in keys_list:
            key = self.bucket.get_key(key_str)
            if not key:
                all_exist = False
                break
        return all_exist

    def download(self, key_str, dest_filename, verify_md5sum=None):
        """ Download to filename. 
        Args:
          key_str (str): get the value stored for this key
          dest_filename (str): where to try to store the file
        Raises: 
          DownloadError if error occurs
        """
        key = self.bucket.get_key(key_str)
        
        if verify_md5sum is not None:
            if verify_md5sum != key.etag:
                raise ValueError('Md5sum does not match: %s %s' % (verify_md5sum, key.etag))
            
        if key:
            f = open(dest_filename, 'wb')
            key.get_file(f)
        else:
            raise S3DownloadError('key not found: %s' % key_str)
        
        return

    def mime_upload(self, src_filename, dest_key):
        """ Upload the file data with dest_key, setting mime-times derived from source file extensions. """
        entry = s3_key.Key(self.bucket)
        entry.key = dest_key
        try:
            f = open(src_filename, 'rb')
        except IOError:
            LOG(INFO, 'File error: %s' % (src_filename))
            raise
        _, extension = os.path.splitext(src_filename)
        headers = {}
        if extension in mimetypes.types_map:
            headers['Content-Type'] = mimetypes.types_map[extension]
        LOG(INFO, 'uploading: %s to %s/%s' % (src_filename, self.bucket, dest_key))
        entry.set_contents_from_file(f, reduced_redundancy = self._reduced_redundancy, headers=headers)
        return
    
    def mime_upload_directory(self, src_dir, dest_key_prefix):
        CHECK(os.path.isdir(src_dir))
        
        src_filenames = glob.glob('%s/*' % (src_dir))
        
        for src_filename in src_filenames:
            filename_base = os.path.basename(src_filename)
            key = '%s/%s' % (dest_key_prefix, filename_base)            
            self.mime_upload(src_filename, key)
        return
            
        
        

    def __repr__(self):
        return '<S3FileStore bucket: %s>' % (self._bucket_name)


class WebInterface(BaseInterface):

    """ Get urls for value from a key. """

    def __init__(self, bucket_name):
        super(WebInterface, self).__init__(bucket_name)
        return

    def create_public_url(self, key_str):
        """ Generate a public url for this key's value 
        Args:
          key_str (str): get the value stored for this key          

        """
        url = None
        key = self.bucket.get_key(key_str)
        if key:
            url = key.generate_url(expires_in=600000)  # ~ about a week
        return url

    def __repr__(self):
        return '<S3WebInterface bucket: %s>' % (self._bucket_name)


class S3DownloadError(Exception):

    """Exception raised for errors downloading a file from s3. """

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return '<S3DownloadError: %s>' % (self.msg)
