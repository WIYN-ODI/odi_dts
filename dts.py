
import os
import subprocess
import glob
import hashlib
import pyfits
import time
import shutil
import logging
import psutil

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
                 cleanup=True,
                 ppa=None,
                 extra=None,
                 ):

        self.logger = logging.getLogger(obsid if obsid is not None else "??????")
        self.database = database
        self.ppa = ppa
        self.extra = extra

        if (exposure_directory is None and obsid is not None):
            # This is a special case to make re-send exposures easier, as OBSID
            # is easy to extract from PPA, while the directory is something very
            # specific to the ODI archive at WIYN
            try:
                exposure_directory = self.database.get_directory_from_obsid(obsid)
            except ValueError:
                self.logger.error("Unable to find exposure directory for %s" % (obsid))
                # no valid directory found
                return

        if (not os.path.isdir(exposure_directory)):
            raise ValueError("Input needs to be an existing directory")

        if (exposure_directory.endswith("/")):
            exposure_directory = exposure_directory[:-1]

        self.exposure_directory = exposure_directory
        self.scratch_dir = scratch_dir if scratch_dir is not None else config.tar_scratchdir
        _, self.dir_name = os.path.split(exposure_directory)

        self.get_filelist()

        self.obsid = obsid
        if (self.obsid is None):
            self.update_obsid_from_files()

        # prepare the logger
        self.logger = logging.getLogger(self.obsid)

        self.logger.info("Reading files from %s" % (self.dir_name))

        self.tar_directory = os.path.join(self.scratch_dir, self.dir_name)
        if (not os.path.isdir(self.tar_directory)):
            self.logger.info("Creating tar-directory: %s" % (self.tar_directory))
            os.mkdir(self.tar_directory)

        self.tar_filename = os.path.join(self.scratch_dir, self.dir_name)+".tar"

        self.transfer_protocol = transfer_protocol

        if (remote_target is None):
            self.remote_target_directory = "%s:%s" % (config.remote_server, config.remote_directory)
        else:
            self.remote_target_directory = remote_target

        self.tar_checksum = None
        self.tar_filesize = -1
        self.tar_transfer_time = -1
        self.archive_ingestion_message = None

        self.cleanup_when_complete = cleanup
        self.cleanup_filelist = []
        self.cleanup_directories = []

        self.ppa_send = "send"
        self.ppa_send_complete = "send_complete"
        if (self.extra == "resend"):
            self.ppa_send = "resend"
            self.ppa_send_complete = "resend_complete"

        if (auto_start):
            self.archive()

    def update_obsid_from_files(self):

        # open each of the files and search for the OBSID keyword
        print("Updating OBSID from data")
        for (fn,_,_,_) in self.filelist:
            with pyfits.open(fn) as hdulist:
                # hdulist.info()
                for ext in hdulist:
                    if ('OBSID' in ext.header):
                        self.obsid = ext.header['OBSID']
                        #print("FOUND OBSID: %s" % (self.obsid))
                        return
        return


    def archive(self):
        all_steps_successful = False
        self.ppa.report_exposure(obsid=self.obsid, msg_type=self.ppa_send,)
        if (self.make_tar()):
            if (self.transfer_to_archive()):
                if (self.report_new_file_to_archive()):
                    self.register_transfer_complete()
                    self.logger.info("All successful")
                    all_steps_successful = True
                    self.ppa.report_exposure(obsid=self.obsid, msg_type=self.ppa_send_complete)
        if (not all_steps_successful):
            self.mark_as_tried_and_failed()
            # TODO: ADD MORE INFO ABOUT ERROR
            self.ppa.report_exposure(obsid=self.obsid, msg_type="error",
                                     comment="Error during transport WIYN-->PPA. Check logs for now, detailed error message not yet implemented.")

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
            self.filelist.append([os.path.abspath(ff), bn+".fz", True, True])

        # collect all expVideo files - these go in a sub-directory
        expvideo_files = glob.glob(
            os.path.join(os.path.join(self.exposure_directory, "expVideo"), "*.fits")
        )
        for ff in expvideo_files:
            _, bn = os.path.split(os.path.abspath(ff))
            self.filelist.append([os.path.abspath(ff), os.path.join("expVideo", bn)+".fz", True, True])

        # also include the metainf.xml
        self.filelist.append([os.path.join(self.exposure_directory, "metainf.xml"), "metainf.xml", False, False])

        # print("\n".join(fits_files))
        # self.filelist = fits_files

    def make_tar(self):

        # run fpack to enable compression on all image files
        self.logger.info("Compressing data")
        md5_data = []
        for file_info in self.filelist: #fits_files:
            #print(file_info)
            (in_file, out_file, compress, include_md5) = file_info
            if (compress):
                dir,bn = os.path.split(out_file)
                sub_directory = os.path.join(self.tar_directory, dir)
                if (dir != '' and not os.path.isdir(sub_directory)):
                    os.mkdir(sub_directory)
                    self.cleanup_directories.append(sub_directory)

                fz_file, md5, returncode = self.fpack(in_file, out_file)
                if (returncode != 0):
                    return False

                self.logger.info("compressing %s to %s" % (in_file, out_file))
                if (include_md5):
                    md5_data.append("%s %s" % (md5, fz_file))
                self.cleanup_filelist.append(os.path.join(self.tar_directory, out_file))
            else:
                full_out = os.path.join(self.tar_directory, out_file)
                try:
                    shutil.copy(in_file,full_out)
                    self.logger.info("Copying %s to tar-prep directory" % (in_file))
                    if (include_md5):
                        md5 = self.calculate_checksum(full_out)
                        md5_data.append("%s %s" % (md5, out_file))
                except IOError as e:
                    self.logger.error("I/O Error (%d) while copying %s: %s" % (e.errno, in_file, e.strerror))
                    # likely caused by file-not-found
                    pass
                self.cleanup_filelist.append(full_out)

        if (len(md5_data) <= 0):
            # no data here
            return False

        # create the md5.txt file
        md5_filename = os.path.join(os.path.join(self.scratch_dir, self.dir_name),
                                    "md5.txt")
        # print(md5_data)
        with open(md5_filename, "w") as md5f:
            md5f.write("\n".join(md5_data))
        self.cleanup_filelist.append(md5_filename)

        # Now create the actual tar ball
        self.logger.info("Making tar ball (%s)" % (self.tar_filename))
        tar_cmd = "tar --create --seek --file=%s --directory=%s %s" % (
            self.tar_filename, self.scratch_dir, self.dir_name)
        # print(tar_cmd)
        returncode = self.execute(tar_cmd)
        if (returncode != 0):
            return False
        #--remove-files

        self.tar_checksum = self.calculate_checksum(self.tar_filename)
        self.tar_filesize = os.path.getsize(self.tar_filename)
        self.logger.info("Resulting tar-ball: %d bytes, MD5=%s" % (self.tar_filesize, self.tar_checksum))
        # print(self.tar_checksum)

        return True

    def fpack(self, filename, outfile):
        #_, basename = os.path.split(filename)
        #fz_filename = basename+".fz"
        fz_filename_full = os.path.join(self.tar_directory, outfile)
        # cmd = "fpack -S %s > %s" % (filename, fz_filename_full)
        # # print(cmd)
        # returncode = self.execute(cmd, monitor=False)

        cmd = "fpack -S %s" % (filename)
        returncode = self.execute(cmd, monitor=False, redirect_stdout=fz_filename_full)

        checksum = self.calculate_checksum(fz_filename_full)
        return outfile, checksum, returncode

    def transfer_to_archive(self):

        if (self.transfer_protocol == 'scp'):
            cmd = "scp %s %s" % (self.tar_filename, self.remote_target_directory)
        elif (self.transfer_protocol == 'rsync'):
            cmd = "rsync -avu --progress %s %s" % (self.tar_filename, self.remote_target_directory)
        else:
            raise ValueError("Could not identfy which transfer protocal to use")

        self.logger.info("Copying to archive using %s (%s)" % (self.transfer_protocol, cmd))
        # print(cmd)
        start_time = time.time()
        returncode = self.execute(cmd)
        end_time = time.time()
        self.tar_transfer_time = end_time - start_time
        self.logger.info("Done with transfer, time=%.1f seconds, bandwidth: %d bytes/sec" % (
            self.tar_transfer_time, self.tar_filesize//self.tar_transfer_time
        ))
        return (returncode == 0)


    def register_transfer_complete(self):
        # print("Marking as complete")
        extra_formatted = '' if self.extra is None else "%s " % (self.extra)
        event = "pyDTS %s%s: fpack(%d) - tar(%5.1fMB, MD5=%s) - transfer(%4.1fs @ %5.2fMB/s via %s) - upload(%s): OK :: 0" % (
            extra_formatted, self.obsid,
            len(self.filelist), self.tar_filesize/2**20, self.tar_checksum,
            self.tar_transfer_time, self.tar_filesize/2**20/self.tar_transfer_time,
            self.transfer_protocol, self.remote_target_directory,
        )
        self.database.mark_exposure_archived(self.obsid, event=event)
        self.logger.info("Adding event to database: %s" % (event))
        pass


    def mark_as_tried_and_failed(self):
        event = "pyDTS %s: data transfer tried but failed: ERROR :: -1" % (
            self.obsid,
        )
        self.database.mark_exposure_archived(self.obsid, event=event)
        self.logger.info("Adding event to database: %s" % (event))


    def execute(self, cmd, monitor=True, redirect_stdout=None):
        try:
            stdout = subprocess.PIPE
            if (redirect_stdout is not None):
                stdout = open(redirect_stdout, "wb")
            ret = subprocess.Popen(cmd.split(),
                                   stdout=stdout,
                                   stderr=subprocess.PIPE)

            if (monitor):
                #
                # Wait for sextractor to finish (or die)
                #
                ps = psutil.Process(ret.pid)
                execution_error = False
                while(True):
                    try:
                        ps_status = ps.status()
                        if (ps_status in [psutil.STATUS_ZOMBIE,
                                          psutil.STATUS_DEAD] and
                            ret.poll() is not None):
                            self.logger.critical("Command died unexpectedly (%s ==> %s)" % (
                                cmd, str(ps_status)))
                            execution_error = True
                            break
                    except psutil.NoSuchProcess:
                        pass

                    if (ret.poll() is None):
                        # self.logger.debug("Command completed successfully!")
                        break
                    time.sleep(0.05)
            else:
                execution_error = False

            if (not execution_error):
                (cmd_stdout, cmd_stderr) = ret.communicate()
                if (ret.returncode != 0):
                    self.logger.warning("Command might have a problem, check the log")
                    self.logger.debug("Stdout=\n"+cmd_stdout)
                    self.logger.debug("Stderr=\n"+cmd_stderr)

        except OSError as e:
            self.logger.critical("Execution failed: %s" % (str(e)))
        end_time = time.time()

        # logger.debug("SourceExtractor returned after %.3f seconds" % (end_time - start_time))

        if (redirect_stdout is not None):
            stdout.close()

        # os.system(cmd)
        return ret.returncode


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
        return True

    def cleanup_files(self):

        # first, delete all files
        for fn in self.cleanup_filelist:
            try:
                if (os.path.isfile(fn)):
                    os.remove(fn)
            except OSError:
                self.logger.error("ERROR deleting file %s" % fn)

        # also delete all dub-directories
        for dn in self.cleanup_directories:
            try:
                if (os.path.isdir(dn)):
                    os.rmdir(dn)
            except OSError:
                self.logger.error("ERROR deleting directory %s - not empty?" % dn)

        # and lastly, delete the directory for the tar-ball, followed by the tar-ball itself
        if (os.path.isfile(self.tar_filename)):
            try:
                os.remove(self.tar_filename)
            except OSError:
                self.logger.error("ERROR deleting tar-ball %s" % self.tar_filename)

        if (os.path.isdir(self.tar_directory)):
            try:
                os.rmdir(self.tar_directory)
            except OSError:
                self.logger.error("ERROR deleting temp directory %s" % (self.tar_directory))

        self.logger.info("Done cleaning up files and directories")
        pass
