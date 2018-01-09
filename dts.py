
import os
import subprocess
import glob
import hashlib
import pyfits
import time
import shutil

import config

class DTS ( object ):


    def __init__(self,
                 exposure_directory,
                 obsid=None,
                 database=None,
                 scratch_dir=None,
                 transfer_protocol='rsync',
                 auto_start=True,
                 remote_target=None,
                 cleanup=True):

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

        if (remote_target is None):
            self.remote_target_directory = "%s:%s" % (config.remote_server, config.remote_directory)
        else:
            self.remote_target_directory = remote_target

        self.tar_checksum = None
        self.obsid = obsid
        if (self.obsid is None):
            self.update_obsid_from_files()

        self.archive_ingestion_message = None

        self.cleanup_when_complete = cleanup
        self.cleanup_filelist = []
        self.cleanup_directories = []

        self.database = database
        if (auto_start):
            self.archive()

    def update_obsid_from_files(self):

        # open each of the files and search for the OBSID keyword
        print("Updating OBSID from data")
        for (fn,_,_) in self.filelist:
            with pyfits.open(fn) as hdulist:
                # hdulist.info()
                for ext in hdulist:
                    if ('OBSID' in ext.header):
                        self.obsid = ext.header['OBSID']
                        #print("FOUND OBSID: %s" % (self.obsid))
                        return
        return

    def archive(self):
        self.make_tar()
        self.transfer_to_archive()
        self.report_new_file_to_archive()
        self.register_transfer_complete()
        if (self.cleanup_when_complete):
            self.cleanup_files()

    def get_filelist(self):
        # Check all files in the directory - we need to collect all FITS files

        self.filelist = []

        # collect all FITS files
        wildcards = os.path.join(self.exposure_directory, "*.fits")
        fits_files = glob.glob(wildcards)
        for ff in fits_files:
            _,bn = os.path.split(os.path.abspath(ff))
            self.filelist.append([os.path.abspath(ff), bn+".fz", True])

        # collect all expVideo files - these go in a sub-directory
        expvideo_files = glob.glob(
            os.path.join(os.path.join(self.exposure_directory, "expVideo"), "*.fits")
        )
        for ff in expvideo_files:
            _, bn = os.path.split(os.path.abspath(ff))
            self.filelist.append([os.path.abspath(ff), os.path.join("expVideo", bn)+".fz", True])

        # also include the metainf.xml
        self.filelist.append([os.path.join(self.exposure_directory, "metainf.xml"), "metainf.xml", False])

        # print("\n".join(fits_files))
        # self.filelist = fits_files

    def make_tar(self):

        # run fpack to enable compression on all image files
        print("Compressing data")
        md5_data = []
        for file_info in self.filelist: #fits_files:
            print(file_info)
            (in_file, out_file, compress) = file_info
            if (compress):
                dir,bn = os.path.split(out_file)
                sub_directory = os.path.join(self.tar_directory, dir)
                if (dir != '' and not os.path.isdir(sub_directory)):
                    os.mkdir(sub_directory)
                    self.cleanup_directories.append(sub_directory)

                fz_file, md5 = self.fpack(in_file, out_file)
                md5_data.append("%s %s" % (md5, fz_file))
                self.cleanup_filelist.append(os.path.join(self.tar_directory, out_file))
            else:
                full_out = os.path.join(self.tar_directory, out_file)
                shutil.copy(in_file,full_out)
                md5 = self.calculate_checksum(full_out)
                md5_data.append("%s %s" % (md5, out_file))
                self.cleanup_filelist.append(full_out)

        # create the md5.txt file
        md5_filename = os.path.join(os.path.join(self.scratch_dir, self.dir_name),
                                    "md5.txt")
        print(md5_data)
        with open(md5_filename, "w") as md5f:
            md5f.write("\n".join(md5_data))
        self.cleanup_filelist.append(md5_filename)

        # Now create the actual tar ball
        print("Making tar ball")
        tar_cmd = "tar --create --seek --file=%s --directory=%s %s" % (
            self.tar_filename, self.scratch_dir, self.dir_name)
        # print(tar_cmd)
        self.execute(tar_cmd)
        #--remove-files

        self.tar_checksum = self.calculate_checksum(self.tar_filename)
        self.tar_filesize = os.path.getsize(self.tar_filename)

        # print(self.tar_checksum)

        pass

    def fpack(self, filename, outfile):
        #_, basename = os.path.split(filename)
        #fz_filename = basename+".fz"
        fz_filename_full = os.path.join(self.tar_directory, outfile)
        cmd = "fpack -S %s > %s" % (filename, fz_filename_full)
        # print(cmd)
        self.execute(cmd)
        checksum = self.calculate_checksum(fz_filename_full)
        return outfile, checksum

    def transfer_to_archive(self):

        if (self.transfer_protocol == 'scp'):
            cmd = "scp %s %s" % (self.tar_filename, self.remote_target_directory)
        elif (self.transfer_protocol == 'rsync'):
            cmd = "rsync -avu --progress %s %s" % (self.tar_filename, self.remote_target_directory)
        else:
            raise ValueError("Could not identfy which transfer protocal to use")

        print("Copying to archive using %s (%s)" % (self.transfer_protocol, cmd))
        # print(cmd)
        start_time = time.time()
        self.execute(cmd)
        end_time = time.time()
        self.tar_transfer_time = end_time - start_time

        pass

    def register_transfer_complete(self):
        # print("Marking as complete")
        event = "pyDTS %s: fpack(%d) - tar(%5.1fMB, MD5=%s) - transfer(%4.1fs @ %5.2fMB/s via %s) - upload(%s): OK :: 0" % (
            self.obsid,
            len(self.filelist), self.tar_filesize/2**20, self.tar_checksum,
            self.tar_transfer_time, self.tar_filesize/2**20/self.tar_transfer_time,
            self.transfer_protocol, self.remote_target_directory,
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

    def report_new_file_to_archive(self):
        return

    def cleanup_files(self):

        # first, delete all files
        for fn in self.cleanup_filelist:
            try:
                if (os.path.isfile(fn)):
                    os.remove(fn)
            except OSError:
                print("ERROR deleting file %s" % fn)

        # also delete all dub-directories
        for dn in self.cleanup_directories:
            try:
                if (os.path.isdir(dn)):
                    os.removedirs(dn)
            except OSError:
                print("ERROR deleting directory %s - not empty?" % dn)

        # and lastly, delete the directory for the tar-ball, followed by the tar-ball itself
        if (os.path.isfile(self.tar_filename)):
            try:
                os.remove(self.tar_filename)
            except OSError:
                print("ERROR deleting tar-ball %s" % self.tar_filename)

        if (os.path.isdir(self.tar_directory)):
            try:
                os.removedirs(self.tar_directory)
            except OSError:
                print("ERROR deleting temp directory %s" % (self.tar_directory))

        pass
