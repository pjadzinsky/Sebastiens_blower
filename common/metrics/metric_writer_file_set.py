import shutil
import uuid
import datetime
from common.metrics import metric_file_naming
from common.metrics.metric_file import MetricFile
import os.path

#Utility for creating and opening-for-write multiple .metrics files as destinations for writing arbitrary metric values.
#The files are created with names and paths conforming to the key naming conventions on the S3 slab data bucket
class MetricWriterFileSet:
    def __init__(self, root_path):
        self.root_path = root_path
        self.writers = {}
        self.custom_timestamp = None


    def __enter__(self):
        if os.path.exists(self.root_path):
            assert os.listdir(self.root_path) == [], "root_path (%s) must be empty since it is deleted when MetricWriterFileSet exits scope" % self.root_path
        else:
            os.makedirs(self.root_path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_all_writers()
        shutil.rmtree(self.root_path)


    def get_or_create_writer(self, metric_name, source_id, timestamp):
        directory, filename = metric_file_naming.get_metric_filename(metric_name, source_id, timestamp)
        key = os.path.join(directory, filename)
        writer = self.writers.get(key)
        if not writer:
            directory = os.path.join(self.root_path, directory)
            suffix = self.get_suffix()
            filename += suffix
            full_path = os.path.join(directory, filename)
            try:
                if not os.path.exists(directory):
                    os.makedirs(directory)
                file = open(full_path, "w")
            except IOError:
                #if too many open files, close all writers and try again
                self.close_all_writers()
                if not os.path.exists(directory):
                    os.makedirs(directory)
                file = open(full_path, "a")

            writer = MetricFile(file, full_path)
            self.writers[key] = writer
        return writer

    def get_suffix(self):
        if self.custom_timestamp:
            assert metric_file_naming.SEPARATOR not in self.custom_timestamp
            timestamp = self.custom_timestamp
        else:
            # append microsecond datetime to avoid any key collisions and keep datapoints sorted by creation time
            timestamp = datetime.datetime.utcnow().strftime("%Y.%m.%d.%H.%M.%S.%f")
        suffix = "%s%s.metrics" % (metric_file_naming.SEPARATOR, timestamp)
        return suffix

    @staticmethod
    def get_creation_timestamp(filename):
        """
        :return: Given a filename, get the creation timestamp
        """
        suffix=filename[filename.rfind(metric_file_naming.SEPARATOR)+1:]
        # suffix = filename.split(metric_file_naming.SEPARATOR, 1)[-1]
        assert suffix.endswith(".metrics")
        return suffix.replace(".metrics", "")

    def close_all_writers(self):
        for key in self.writers.iterkeys():
            writer = self.writers[key]
            writer.file.close()
        self.writers = {}