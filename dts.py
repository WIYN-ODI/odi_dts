
import os
import subprocess
import glob
import hashlib
import pyfits
import time

class DTS ( object ):


    def __init__(self,
                 exposure_directory,
                 obsid=None,
                 database=None,
                 scratch_dir=None,
                 transfer_protocol='rsync',
                 auto_start=True):

        if (not os.path.isdir(exposure_directory)):
            raise ValueError("Input needs to be an existing directory")

        if (exposure_directory.endswith("/")):
            exposure_directory = exposure_directory[:-1]

        self.exposure_directory = exposure_directory
        self.scratch_dir = scratch_dir if scratch_dir is not None else "/tmp"
        _, self.dir_name = os.path.split(exposure_directory)
        print(self.dir_name)

        self.get_filelist()

        self.tar_directory = os.path.join(self.scratch_dir, self.dir_name)
        if (not os.path.isdir(self.tar_directory)):
            print("Creating tar-directory: %s" % (self.tar_directory))
            os.mkdir(self.tar_directory)

        self.tar_filename = os.path.join(self.scratch_dir, self.dir_name)+".tar"

        self.transfer_protocol = transfer_protocol
        self.remote_target_directory = "galev.org:/sas/misc"
        self.tar_checksum = None
        self.obsid = obsid
        if (self.obsid is None):
            self.update_obsid_from_files()

        self.database = database
        if (auto_start):
            self.archive()

    def update_obsid_from_files(self):

        # open each of the files and search for the OBSID keyword
        print("Updating OBSID from data")
        for fn in self.filelist:
            with pyfits.open(fn) as hdulist:
                hdulist.info()
                for ext in hdulist:
                    if ('OBSID' in ext.header):
                        self.obsid = ext.header['OBSID']
                        print("FOUND OBSID: %s" % (self.obsid))
                        return
        return

    def archive(self):
        self.make_tar()
        self.transfer_to_archive()
        self.register_transfer_complete()

    def get_filelist(self):
        # Check all files in the directory - we need to collect all FITS files
        wildcards = os.path.join(self.exposure_directory, "*.fits")
        fits_files = glob.glob(wildcards)
        print("\n".join(fits_files))
        self.filelist = fits_files

    def make_tar(self):
        print("Making tar ball")

        # run fpack to enable compression on all image files
        for filename in self.filelist: #fits_files:
            self.fpack(filename)

        # Now create the actual tar ball
        tar_cmd = "tar --create --seek --file=%s --directory=%s %s" % (
            self.tar_filename, self.scratch_dir, self.dir_name)
        print(tar_cmd)
        self.execute(tar_cmd)
        #--remove-files

        self.tar_checksum = self.calculate_checksum(self.tar_filename)
        self.tar_filesize = os.path.getsize(self.tar_filename)

        print(self.tar_checksum)

        pass

    def fpack(self, filename):
        _, basename = os.path.split(filename)
        fz_filename = basename+".fz"
        cmd = "fpack -S %s > %s" % (filename, os.path.join(self.tar_directory, fz_filename))
        print(cmd)
        self.execute(cmd)
        return

    def transfer_to_archive(self):

        if (self.transfer_protocol == 'scp' or True):
            cmd = "scp %s %s" % (self.tar_filename, self.remote_target_directory)
        elif (self.transfer_protocol == 'rsync'):
            cmd = "rsync -avu --progress %s %s" % (self.tar_filename, self.remote_target_directory)
        else:
            raise ValueError("Could not identfy which transfer protocal to use")

        print("Copying to archive")
        print(cmd)
        start_time = time.time()
        self.execute(cmd)
        end_time = time.time()
        self.tar_transfer_time = end_time - start_time

        pass

    def register_transfer_complete(self):
        print("Marking as complete")
        event = "pyDTS: fpack(%d) - tar(%.1fMB, MD5=%s) - transfer(%.1fs @ %.2fMB/s) - ingest: OK :: -1" % (
            len(self.filelist), self.tar_filesize/2**20, self.tar_checksum,
            self.tar_transfer_time, self.tar_filesize/2**20/self.tar_transfer_time
        )
        self.database.mark_exposure_archived(self.obsid, event=event)
        pass

    def execute(self, cmd):
        os.system(cmd)

    def calculate_checksum(self, fn):

        # from https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
        def hash_bytestr_iter(bytesiter, hasher, ashexstr=False):
            for block in bytesiter:
                hasher.update(block)
            return hasher
        def file_as_blockiter(afile, blocksize=65536):
            with afile:
                block = afile.read(blocksize)
                while len(block) > 0:
                    yield block
                    block = afile.read(blocksize)
        checksum = hash_bytestr_iter(file_as_blockiter(open(fn, 'rb')),
                                     hashlib.md5())
        # print("MD5v1:", checksum.hexdigest())
        # checksum = hashlib.md5(open(fn, 'rb').read()).hexdigest()
        return checksum.hexdigest()

        # [(fname, hash_bytestr_iter(file_as_blockiter(open(fname, 'rb')), hashlib.md5()))
        #  for fname in fnamelst]