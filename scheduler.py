import Queue
import threading
import time
import json
from logging import getLogger, StreamHandler, Formatter, DEBUG, INFO, ERROR, FileHandler, NullHandler
import traceback
import os
import sys
import uuid

Q_JSON_FILENAME = 'qu.json'
RESULT_PATH = '/home/muneel/Documents/results/'

class MLOGGER:
    @staticmethod
    def get_logger(name):
        if not name:
            raise ValueError('Name parameter can not be empty.')
        return MLOGGER(name)

    @staticmethod
    def __create_stream_handler(level):
        handler = StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(Formatter('%(asctime)s - %(levelname)s - %(instance_id)s - %(message)s','%Y-%m-%d %H:%M:%S'))
        return handler

    @staticmethod
    def __create_file_handler(level, filename):
        filename_path = str(os.path.dirname(os.path.realpath(__file__))) + '/' + str(filename)
        fileHandler = FileHandler(filename_path, mode='w')
        fileHandler.setLevel(level)
        fileHandler.setFormatter(Formatter('%(asctime)s - %(levelname)s - %(instance_id)s - %(message)s','%Y-%m-%d %H:%M:%S'))
        return fileHandler

    def __init__(self, name, level = INFO, logtype = 'CONSOLE', filename = None):
        # logtype  : {'CONSOLE', 'FILE', 'BOTH', 'NONE'}
        # level : {INFO, DEBUG, ERROR}
        self.user_variables = {}
        self.user_variables['instance_id'] = self.__class__.__name__
        self.logger = getLogger(name)
        self.logger.setLevel(level)
        if logtype == 'CONSOLE':
            self.logger.addHandler(MLOGGER.__create_stream_handler(level))
        elif logtype == 'FILE' :
            if filename is not None:
                self.logger.addHandler(MLOGGER.__create_file_handler(level,filename))
            else:
                raise ValueError('filename cannot be empty')
                sys.exit()
        elif logtype == 'BOTH':
            self.logger.addHandler(MLOGGER.__create_stream_handler(level))
            if filename is not None:
                self.logger.addHandler(MLOGGER.__create_file_handler(level,filename))
            else:
                raise ValueError('filename cannot be empty')
                sys.exit()
        elif logtype == 'NONE':
            self.logger.addHandler(NullHandler())

    def __set_message(self, message):
        tb = traceback.extract_stack()
        return(tb[1][2] + ' - ' + message)

    def debug(self, message):
        self.logger.debug(self.__set_message(message), extra=self.user_variables)

    def info(self, message):
        self.logger.info(self.__set_message(message), extra=self.user_variables)

    def warn(self, message):
        self.logger.warn(self.__set_message(message), extra=self.user_variables)

    def error(self, message):
        self.logger.error(self.__set_message(message), extra=self.user_variables)


class Scheduler(MLOGGER):

    def __init__(self):
        MLOGGER.__init__(self,'Scheduler', level=DEBUG, logtype='CONSOLE', filename = 'scheduler_log.log')
        self.q = Queue.Queue()
        self.lock = False

    def load_json(self, filename):
        try:
            with open(filename) as data_file:
                data = json.load(data_file)
            data_file.close()
            return data
        except Exception as e:
            self.error('%s' %e)

    def dump_json(self, filename, data):
        try:
            with open(filename, 'w') as outfile:
                json.dump(data, outfile)
            outfile.close()
        except Exception as e:
            self.error('%s' %e)

    def add_work(self, work, load_work = False):
        # Add work recieved into the queue
        # Before adding into the queue it prepares by adding status
        # And uid
        self.info('Recieved Work %s' %work)
        if load_work is False:
            self.lock_queue()
            work = self.prepare_work(work)
            data = self.load_json(Q_JSON_FILENAME)
            self.debug('Current json queue : %s' %data)
            data.append(work)
            self.debug('Added %s to json file'  %work)
            self.debug('Current json queue : %s' %data)
            self.dump_json(Q_JSON_FILENAME, data)
            self.unlock_queue()
        self.q.put(work)
        return work

    def prepare_work(self, work):
        # This add uid to the work i.e and status
        # Also creates a directory with the same uid under which result.json is created
        # with same information so that it can be accessed later
        uid = str(uuid.uuid4())
        self.debug('uid generated : %s' %uid)
        work.update({'uid' : uid, 'status':'pending'})
        self.debug('work updated : %s' %work)
        os.mkdir(RESULT_PATH + uid)
        self.debug('directory created %s' %(RESULT_PATH + uid))
        json_file = RESULT_PATH + work['uid'] + '/result.json'
        self.dump_json(json_file, work)
        self.debug('result.json created : %s' %json_file)
        return work

    def remove_work(self):
        pass

    def get_work(self):
        # Print and returns the current queue
        self.info('Start of current queue')
        cur_queue = []
        for work in list(self.q.queue):
            self.info('%s' %work)
            cur_queue.append(work)
        self.info('End of current queue')
        return cur_queue

    def get_result(self, work):
        # Return result of the work by accessing it through uid
        self.info('Getting result for %s :' %work['uid'])
        json_file = RESULT_PATH + work['uid'] + '/result.json'
        result = self.load_json(json_file)
        self.debug('Result : %s' %result)
        return result

    def lock_queue(self):
        self.debug('Queue Locked')
        self.lock = True
        return self.lock_queue

    def unlock_queue(self):
        self.debug('Queue Unlocked')
        self.lock = False
        return self.lock_queue

    def work(self):
        # Main Work
        while True:
            while not self.q.empty() and self.lock is False:
                work = self.q.get()
                self.info('Working on : %s' %work)
                data = self.load_json(Q_JSON_FILENAME)
                print data
                data.pop(0)
                self.debug('Current json file data: %s' %data)
                self.dump_json(Q_JSON_FILENAME, data)
                json_file = RESULT_PATH + work['uid'] + '/result.json'
                time.sleep(10)
                work['status'] = 'done'
                self.dump_json(json_file, work)
                self.debug('result.json updated : %s' %json_file)

                # Create directory

    def start_work(self):
        # Starts working Thread
        self.debug('Starting Scheduler work thread')
        t = threading.Thread(name = "ConsumerThread-", target=self.work, args=())
        t.daemon = True
        t.start()

    def load_work(self):
        # Load work from json file only 1st time of starting
        self.info('Loading work from json file')
        data = self.load_json(Q_JSON_FILENAME)
        self.debug('Data to be added to queue : %s' %data)
        self.add_works(data)

    def add_works(self, data):
        # Add multiple work into the queue
        for d in data:
            self.add_work(d, True)
        return
'''
s = Scheduler()
s.load_work()
#s.add_work({'name' : 'karrar'})
s.get_work()
s.start_work()
time.sleep(10)
w = s.add_work({'name' : 'khawaja'})
s.get_work()
time.sleep(5)
#print s.get_result(w)

while True:
    time.sleep(1)
'''
