
import logging
import sys
import threading
import queue
import traceback

class QueueHandler(logging.Handler):

    def __init__(self, queue):
        logging.Handler.__init__(self)
        self.queue = queue
        self.msgcount = 0

    def flush(self):
        pass

    def emit(self, record):
        self.msgcount += 1

        try:
            self.queue.put_nowait(record)
        except AssertionError:
            pass
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


class DTS_LogHandler(threading.Thread):
    """Threaded Url Grab"""
    def __init__(self, queue, log_filename):
        threading.Thread.__init__(self)
        self.log_queue = queue
        self.log_filename = log_filename

        self.root_logger = logging.getLogger()
        for h in self.root_logger.handlers:
            self.root_logger.removeHandler(h)
        self.root_logger.setLevel(logging.DEBUG)
        self.root_logger.propagate = False
        # try:
        #     h = logging.NullHandler()  # StreamHandler(stream=sys.stdout)
        #     f = logging.Formatter('%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s')
        #     h.setFormatter(f)
        #     self.root_logger.addHandler(h)
        # except AttributeError:
        #     # This happens in older Python versions that don't have a NULLHandler
        #     pass
        # except:
        #     raise
        #
        # self.root_logger.propagate = False

        # debug_logger = root

        self.log = open(self.log_filename, "a")

        # Add the stream handler to write to both terminal and logfile
        self.logger = logging.getLogger("DTS")
        self.logger.setLevel(logging.INFO)
        # debug_logger = logging.getLogger()
        try:
            file_handler = logging.StreamHandler(stream=self.log)
        except TypeError:
            file_handler = logging.StreamHandler(strm=self.log)
        except:
            raise
        # file_formatter = logging.Formatter(
        #     '%(asctime)s -- %(levelname)-8s [ %(filename)30s : %(lineno)4s - %(funcName)30s() in %(processName)-12s] %(name)30s :: %(message)s')
        file_formatter = logging.Formatter(
            '[%(levelname)-8s] %(asctime)s :: %(name)25s :: %(message)s')
        file_handler.setFormatter(file_formatter)
        for h in self.logger.handlers:
            self.logger.removeHandler(h)
        self.logger.addHandler(file_handler)
        self.logger.propagate = False

        #
        # Create a handler for all output that also goes into the display
        #
        try:
            terminal_handler = logging.StreamHandler(stream=sys.stdout)
        except TypeError:
            terminal_handler = logging.StreamHandler(strm=sys.stdout)
        except:
            raise
        # Add some specials to make sure we are always writing to a clean line
        # f = logging.Formatter('\r\x1b[2K%(name)s: %(message)s')
        terminal_formatter = logging.Formatter('%(name)s: %(message)s')
        terminal_handler.setFormatter(terminal_formatter)
        self.logger.addHandler(terminal_handler)


    def run(self):

        msg_received = 0
        while True:
            try:
                try:
                    record = self.log_queue.get(timeout=1.)
                except (KeyboardInterrupt, SystemExit):
                    record = None
                except queue.Empty:
                    pass
                    # print >>debugfile, "LogHandler: still running, but no message during the last second!"
                    # print "."
                    continue
                except:
                    raise

                if (record is None):
                    break

                msg_received += 1
                # Add some logic here

                # print "record-level:",record.levelno, record.levelname, msg_received

                self.logger.handle(record)

                self.log_queue.task_done()
            except (KeyboardInterrupt, SystemExit):
                raise



class dts_logging(object):

    def __init__(self, logfile=None):
        if (logfile is None):
            logfile = "odi_dts.log"
        self.log_filename = logfile

        # root = logging.getLogger()
        # try:
        #     h = logging.StreamHandler(stream=sys.stdout)
        # except TypeError:
        #     h = logging.StreamHandler(strm=sys.stdout)
        # except:
        #     raise
        #     f = logging.Formatter('%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s')
        #     h.setFormatter(f)
        #     root.addHandler(h)

        # Create a queue to serialize all log output from multiple processes
        self.log_queue = queue.Queue()

        # Start the thread that outputs all logs to file and terminal
        self.log_worker = DTS_LogHandler(queue=self.log_queue,
                                         log_filename=self.log_filename)
        self.log_worker.daemon = True
        self.log_worker.name = "DTS_LogHandler"
        self.log_worker.start()
        import time
        time.sleep(0.1)

        # Now configure the root logger to insert all log messages into the queue rather than
        # writing them directly
        self.queue_handler = QueueHandler(queue = self.log_queue)
        self.root_logger = logging.getLogger()
        for h in self.root_logger.handlers:
            self.root_logger.removeHandler(h)
        self.root_logger.setLevel(logging.DEBUG)
        self.root_logger.addHandler(self.queue_handler)
        self.root_logger.propagate = False

    def stop(self):
        self.log_queue.put(None)
        self.log_queue.join()
