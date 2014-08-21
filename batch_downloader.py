#!/usr/bin/env python 
import Queue
import socket
import signal
import threading
import sys
import os
import time
import math
#third-party library
import requests
import eventlet
import threadpool

#global variables
global stop
global log_queue

TIMEOUT = 600
FOLDER_FILE_QUANTITY = 50000

def signal_handler(signal, frame):
    print('You have pressed Ctrl+C, program will stop!')
    global stop
    stop = True

def worker(task_list, dirpath, log):
    global stop
    
    for idx in xrange(len(task_list)):
        if stop:
            break
        try:
            item_id, url = task_list[idx]
            filename = url[url.rfind('/') + 1:]
            if not url:
                break
            fpath = dirpath + '/%05d/' % (item_id / FOLDER_FILE_QUANTITY) + filename
            
            if os.path.exists(fpath):
                continue

            start_time = int(time.time())
            
            timeout = eventlet.Timeout(TIMEOUT)
            try:
                u = requests.get(url)
            except eventlet.Timeout as e:
                print 'Timeout %d, %s,' % (item_id, url),
                msg = '%s\t%s\n' % (type(e), url)
                log.put(msg)
            except Exception, e:
                print '%s, %d, %s,' % (type(e), item_id, url),
                msg = '%s\t%s\n' % (type(e), url)
                log.put(msg)
            else:
                with open(fpath, 'wb') as f:
                    f.write(u.content)
                print 'Finish %d, %s' % (item_id, url),
            finally:
                timeout.cancel()
                
            end_time = int(time.time())
            print '%ds' % (end_time - start_time)
        except Exception, e:
            msg = '%s\t%s\n' % (type(e), url)
            print msg     

def get_url_list(poolsize):
    f_lst = open(sys.argv[1])
    
    url_lists = [[] for e in range(poolsize)]
    idx = 0
    count = 0
    for l in f_lst:
        l = l.strip()
        if not l:
            continue
        url_lists[idx].append([count, l])
        idx = (idx + 1) % poolsize
        count += 1
    f_lst.close()

    return url_lists

class Log_worker(threading.Thread):
    def __init__(self, f_log, queue_log):
        threading.Thread.__init__(self)
        self.log = f_log
        self.queue = queue_log
        self.close = False

    def run(self):
        print 'log worker run.'
        while True:
            try:
                item = self.queue.get(True, 1)
                f_log.write(item)
                self.queue.task_done()
            except:
                if self.close:
                    break
        print 'log worker exit.'

    def stop_thread(self):
        self.close = True


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    eventlet.monkey_patch()

    #parameter
    poolsize = 100

    #init
    stop = False
    log_queue = Queue.Queue()
    pool = threadpool.ThreadPool(poolsize)    

    if len(sys.argv) >= 3:
        urls = get_url_list(poolsize)
        print 'Url list loaded.'

        #start log 
        f_log = open(sys.argv[2], 'a')
        log = Log_worker(f_log, log_queue)
        log.daemon = True
        log.start()

        #create tasks
        global dirpath
        dirpath = './data/' + os.path.basename(sys.argv[1])[:-4]
        if not os.path.isdir(dirpath):
            os.makedirs(dirpath)

        file_quantity = sum([len(e) for e in urls])
        for idx in range(int(math.ceil(1.0 * file_quantity / FOLDER_FILE_QUANTITY))):
            if not os.path.exists(dirpath + '/%05d' % idx):
                os.mkdir(dirpath + '/%05d' % idx)
        
        thread_requests = threadpool.makeRequests(worker, [([urls[e]], {'dirpath':dirpath, 'log':log_queue}) for e in xrange(len(urls))])
        [pool.putRequest(req) for req in thread_requests]
        
        pool.wait()

        log.stop_thread()
        log.join()

    else:
        print 'Usage: batch_downloader.py urllist.lst urllist.log'


