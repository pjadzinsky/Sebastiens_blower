#!/usr/bin/python
import filecmp
import unittest
import shutil
from common.utils.zipper import *
import os


class TestZipper(unittest.TestCase):
    def setUp(self):
        self.paths = []
        self.base_dir = 'test_zip'
        if (os.path.exists(self.base_dir)):
            shutil.rmtree(self.base_dir)
        os.mkdir(self.base_dir)
        for i in range(1,10):
            path = os.path.join(self.base_dir, str(i))
            open(path, 'w').close()
            self.paths.append(path)
        subdir = os.path.join(self.base_dir, "subdir")
        os.mkdir(subdir)
        for i in range(1,10):
            path = os.path.join(subdir, str(i))
            open(path, 'w').close()
            self.paths.append(path)

    def tearDown(self):
        shutil.rmtree(self.base_dir)


    #a pretty flimsy test at this point... just make sure it basically works without testing the different optional args
    def test_zipping(self):
        #Test .zip.gz
        filename = "test.zip.gz"
        self.zip_and_unzip(filename)

        #Test .tar.gz (default extension when no filename provided)
        self.zip_and_unzip(None)

    def zip_and_unzip(self, filename):
        with Zipper(self.paths, self.base_dir, gz_filename=filename) as gz_filename:
            # make sure zipfile exists inside the with statement
            self.assertTrue(os.path.exists(gz_filename))
            extract_dir = 'extract_dir'
            if os.path.exists(extract_dir):
                shutil.rmtree(extract_dir)
            with UnZipper(gz_filename, extract_dir) as unzipped_root_path:
                dircmp = filecmp.dircmp(self.base_dir, extract_dir)
                self.assertFalse(self.is_different(dircmp))
        # make sure zipfile is deleted outside the 'with' statement
        self.assertFalse(os.path.exists(gz_filename))
        self.assertFalse(os.path.exists(unzipped_root_path))

    def is_different(self, dircmp):
        if len(dircmp.diff_files):
            return True
        for subdircmp in dircmp.subdirs.values():
            if self.is_different(subdircmp):
                return True
        return False
    
if __name__ == "__main__":
    unittest.main()




