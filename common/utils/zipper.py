import gzip
import tarfile
import uuid
import zipfile
import shutil
import os


class Zipper:
    # with Zipper(self.paths, self.base_dir) as zipped_filename:
    #     do stuff with the .gz file
    # Back out here, .gz file is deleted unless you specified delete_on_exit
    def __init__(self, source_paths, zip_root_path=None, gz_filename=None, delete_on_exit=True):
        """

        :param source_paths: list of files or directories (directories are zipped recursively)
        :param zip_root_path: root path (zip file with have paths relative to this). defualt is the common prefix of source_paths
        :param gz_filename: name of zipfile (default is a randomly generated uuid.zip.gz)
        :param delete_on_exit: if True, zipfile is deleted when exiting the context
        :return:
        """
        self.gz_filename = gz_filename if gz_filename else str(uuid.uuid4()) + ".tar.gz"
        self.delete_on_exit = delete_on_exit
        self.source_paths = source_paths
        self.zip_root_path = os.path.commonprefix(source_paths) if zip_root_path is None else zip_root_path

    def __enter__(self):
        #Zip the file
        zip_directory = os.path.dirname(self.gz_filename)
        if zip_directory and not os.path.exists(zip_directory):
            os.makedirs(zip_directory)

        if self.gz_filename.endswith(".zip.gz"):
            self.create_zip_gz(zip_directory)
        else:
            self.create_tar_gz(zip_directory)

        return self.gz_filename

    #Create a .zip.gz file containing our files
    def create_zip_gz(self, zip_directory):
        zip_filename = str(uuid.uuid4()) + ".zip"
        zip_full_path = os.path.join(zip_directory, zip_filename)
        with zipfile.ZipFile(zip_full_path, 'w') as zipf:
            for path in self.source_paths:
                if os.path.isfile(path):
                    self.zip_file(path, zipf)
                elif os.path.isdir(path):
                    for root, dirs, files in os.walk(path):
                        for current_file in files:
                            self.zip_file(os.path.join(root, current_file), zipf)
                else:
                    raise ValueError("source_path '%s' is neither a file or directory" % path)

        # gzip the zip
        with open(zip_full_path, 'rb') as f_in, gzip.open(self.gz_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

        # remove .zip file
        os.remove(zip_full_path)

    def zip_file(self, path, zipf):
        assert path.startswith(self.zip_root_path), "File '%s' is not under zipfile root path '%s'" % (path, self.zip_root_path)
        zipf.write(path, arcname=path.replace(self.zip_root_path, ""))

    def __exit__(self, *args):
        if self.delete_on_exit:
            os.remove(self.gz_filename)

    def create_tar_gz(self, zip_directory):
        with tarfile.open(self.gz_filename, "w:gz") as tar:
            for path in self.source_paths:
                assert path.startswith(self.zip_root_path), "File '%s' is not under zipfile root path '%s'" % (path, self.zip_root_path)
                tar.add(path, arcname=path.replace(self.zip_root_path, ""))



# with UnZipper(gz_filename, extract_dir) as unzipped_root_path:
#     do stuff with the files under extract_dir
# Set delete_on_exit to False if you need the files after leaving the context
class UnZipper:
    def __init__(self, gz_filename, unzip_root=None, delete_files_on_exit=True, delete_root_path_on_exit=True):
        """

        :param gz_filename: name of zipfile (default is a randomly generated value)
        :param delete_files_on_exit: if True, zipfile is deleted when exiting the context
        :return:
        """
        self.gz_filename = gz_filename
        self.delete_files_on_exit = delete_files_on_exit
        self.delete_root_path_on_exit = delete_root_path_on_exit
        self.unzip_root = unzip_root if unzip_root else str(uuid.uuid4())
        self.unzipped_files = []

        if os.path.exists(self.unzip_root) and os.listdir(self.unzip_root) != []:
            assert not delete_root_path_on_exit, "Unzipped into a non-empty directory. Set delete_root_path_on_exit to False"

    def __enter__(self):
        if self.unzip_root and not os.path.exists(self.unzip_root):
            os.makedirs(self.unzip_root)

        if self.gz_filename.endswith(".zip.gz"):
            self.extract_zip_gz()
        else:
            self.extract_tar_gz()
        return self.unzip_root

    #Unzip a file in .zip.gz format
    def extract_zip_gz(self):
        zip_directory = os.path.dirname(self.gz_filename)
        if zip_directory and not os.path.exists(zip_directory):
            os.makedirs(zip_directory)
        zip_filename = str(uuid.uuid4()) + ".zip"
        zip_full_path = os.path.join(zip_directory, zip_filename)
        with gzip.open(self.gz_filename, 'rb') as gz_file, open(zip_full_path, "w") as zip_file:
            read_chunk_length = 1024 * 1024
            content = gz_file.read(read_chunk_length)
            while content:
                zip_file.write(content)
                content = gz_file.read(read_chunk_length)
        with zipfile.ZipFile(zip_full_path, 'r') as z:
            for name in z.namelist():
                z.extract(name, self.unzip_root)
                self.unzipped_files.append(os.path.join(self.unzip_root, name))
        os.remove(zip_full_path)

    def __exit__(self, *args):
        if self.delete_files_on_exit:
            for unzipped_file in self.unzipped_files:
                try:
                    os.remove(unzipped_file)
                except:
                    pass    #no biggie, was probably removed by caller
        if self.delete_root_path_on_exit:
            shutil.rmtree(self.unzip_root)



    def extract_tar_gz(self):
        with tarfile.open(self.gz_filename, "r:gz") as tar:
            tar.extractall(path=self.unzip_root)
            for tarinfo in tar:
                if tarinfo.isreg():
                    self.unzipped_files.append(os.path.join(self.unzip_root, tarinfo.name))
