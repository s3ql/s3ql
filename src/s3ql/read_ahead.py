# -*- coding:<utf-8> -*-

'''
read_ahead.py - this file is part of S3QL.

Copyright  2018 Tommaso Massimi <t.massim@cynnyspace.com>

This work can be distributed under the terms of the GNU GPLv3.
'''

"""
 HOW THE READ AHEAD MECHANISM WORKS

  during  a file uplaod, S3QL desects the original file in smaller files named 
  s3ql_data_$obj_id and store (PUT) them on the remote storage (obj_id is a progressive number)

  during a file downlaod, S3QL retrieves the required part of file from the remote storage;
  if a whole file is required, all the s3ql_data_$obj_id sections of the original file 
  are retrieved in sequence, one after the other one

  the READ AHEAD mechanism tries to speed up the download of a file:
  when some consecutive sections of a file are required, 
  it starts some threads which will download some s3ql_data_$obj_id sections 
  of the file also if these section were not yet required

  to do that the READ AHEAD mechanism needs to:
   a) have the list of $obj_id of the donloaded file
   b) be informed about which s3ql_data_$obj_id sections are downloaded;

  a) and b) are implemented in file block_cache.py, 
  all the other READ AHEAD code is in this file
"""

import threading
import time
import os
import shutil

from .logging import logging # Ensure use of custom logger class
from . import BUFSIZE
from .backends.common import NoSuchObject
# standard logger for this module
log = logging.getLogger(__name__)


class ReadAheadDetails:
    '''
    keeps read_ahead variables for one file

    Attributes:
    -----------
    :last_block_read: number of the last block number read in this file
    :consec_block_count: number of consecutive block read for this file
    :RAlast_access: timestamp of the last read access to this file
    :dict_block_no_objid: dict, key=block_no, value = objid
    :block_count_trigger: must become a parameter: threads are launched when consec_block_count >= block_count_trigger
    '''
    def __init__(self, dict_block_no_objid, blockno):
        self.last_block_read = blockno
        self.consec_block_count = 1
        self.RAlast_access = time.time()

        self.dict_block_no_objid = dict_block_no_objid
        try:
            del self.dict_block_no_objid[blockno]        # remove this block from the list of blocks to be downloaded 
        except NameError:
            pass

        self.block_count_trigger = 2 # parameter

    def required(self, blockno):
        '''
        Called when a block is required to be downloaded. 
        If blockno is a new block to download, and if its value is one more the last downloaded blok, 
        increments the consecutive block counter
        '''
        #log.debug("required start blockno=%d\n\tconsec_block_count=%d\n\tlast_blockno_read=%d" % (blockno, self.consec_block_count, self.last_block_read))
        
        self.RAlast_access = time.time()

        if self.last_block_read +1 == blockno:
            self.consec_block_count += 1
            log.debug("required self.consec_block_count=%d, self.last_block_read=%d" % (self.consec_block_count, self.last_block_read))
        elif self.last_block_read == blockno:
            pass
        else:
            self.consec_block_count = 0
            log.debug("required self.consec_block_count=%d, self.last_block_read=%d" % (self.consec_block_count, self.last_block_read))
        self.last_block_read=blockno
        
        if blockno in self.dict_block_no_objid.keys(): # this block will not be downloaded in read_ahead mode
            del self.dict_block_no_objid[blockno]
        log.debug("required end blockno=%d\n\tconsec_block_count=%d\n\tlast_blockno_read=%d\n" % (blockno, self.consec_block_count, self.last_block_read))

    def trigger_on(self):
        '''
        rule declaring when asyncronous GET threads can be started for this file
        '''
        return self.consec_block_count >= self.block_count_trigger
      
    def expired(self):
        '''
        when a file is accessed after a while, it is safer to reload its details form BD.
        This function decide when the cached file details of a file are too old
        '''
      if time.time() - self.RAlast_access > 40:
          return True
      return False


class ReadAhead(object):
    '''
    Read Ahead block logic
    
    Attributes:
    -----------
    :read_ahead_data: dict inode -> ReadAheadDetails
    :nRAThread:     number of threads to start. will become a parameter
    :listObjIDDwnInProgress: list of $obj_id in download
    :threadLock:    thread lock, used to remove elements from listObjIDDwnInProgress
    '''

    def __init__(self):
        self.read_ahead_data = dict()
        self.nRAThread = 25 # param, nMax Threads
        self.threadLock = threading.Lock()
        self.listObjIDDwnInProgress = []

    def block_download_required(self, path, inode, blockno, obj_id, backend_pool):
        """
        called by file block_cache.py when a fileblock is required
        Starts the download threads if there are the condictions to do that
        """
        log.debug("block_download_required: inode=%d, blockno=%d, obj_id=%d, keys=%s" % (inode, blockno, obj_id, self.ReadAheadData.keys()))
        if inode in self.read_ahead_data.keys():
            self.read_ahead_data[inode].required(blockno)
        else:
            log.debug("ahead error!!! new block_download_required: inode=%d, blockno=%d, obj_id=%d, keys=%s" % (inode, blockno, obj_id, self.ReadAheadData.keys()))

        if self.read_ahead_data[inode].trigger_on():
            log.debug ("Trigger on, list down in progr=%s"% self.listObjIDDwnInProgress)
            if obj_id not in self.listObjIDDwnInProgress:
              self.start_threads(obj_id, inode, blockno, backend_pool, path)
              return True; # thread started 
        return False # thread not started

    def start_threads(self, obj_id, inode, blockno, backend_pool, path):
        """
        Starts the download threads and mantaind the list of the file still to be downloaded
        """

        list_objid = [] # list of obj id
        list_bn = []    # list of block number

        # choose the objId to downlaod asynchrously
        # log.debug("runnable RAhead threads: %s" % self.read_ahead_data[inode].dict_block_no_objid.keys())
        for bn in self.read_ahead_data[inode].dict_block_no_objid.keys():
            list_bn.append(bn)
            list_objid.append((bn, self.read_ahead_data[inode].dict_block_no_objid[bn]))
            if len(list_bn) >= self.nRAThread:
                break

        # remove the objId from the list to avoid to re-downlaod them
        for bn in list_bn:
            del self.read_ahead_data[inode].dict_block_no_objid[bn]

        thList=[]
        # create threads
        for (bn, objId) in list_objid:
            #log.debug("creating RAhead thread inode=%d, objid=%d" % (inode, objId))
            self.listObjIDDwnInProgress.append( objId )
            t = threading.Thread(target=self.get_block, args=(bn, objId, inode, backend_pool,path) )
            thList.append(t)
            #log.debug("created  RAhead thread inode=%d, objid=%d" % (inode, objId))

        # start threads
        for thvar in thList:
           thvar.start()

        # waits for the end of all threads
        for thvar in thList:
          thvar.join()


    def get_block(self,bn, obj_id, inode, backend_pool,path):
        """
        Thread function: download the file and give it the right name
        """
        
        filename = os.path.join(path, '%d-%d' % (inode, bn))
        tmpfh = open(filename + '.async_tmp', 'wb')
        log.debug("opened tmpfh %s -> %s"% (tmpfh.name, filename))
        try:
            def do_read(fh):
                tmpfh.seek(0)
                tmpfh.truncate()
                shutil.copyfileobj(fh, tmpfh, BUFSIZE)
        
            log.debug("start RAhead thread inode=%d, blockNo=%d, obj_id=%d, " % (inode, bn, obj_id))

            with backend_pool() as backend:
                try:
                    fn = 's3ql_data_%d' % obj_id
                    backend.perform_read(do_read, fn)
                except NoSuchObject: # the required block could not exist
                    log.debug("RAhead thread: block %d doesn't exists" % (obj_id))
                finally:
                    self.threadLock.acquire()
                    self.listObjIDDwnInProgress.remove(obj_id)
                    log.debug("end RAhead thread inode=%d, bn=%d, objid=%d, file %s downloaded (%s)" % (inode, bn, obj_id, fn, self.listObjIDDwnInProgress))
                    self.threadLock.release()
              
            os.rename(tmpfh.name, filename)
            # log.debug("renamed %s -> %s"% (tmpfh.name, filename)) 
        except:
            os.unlink(tmpfh.name)
            raise
        finally:
            tmpfh.close()
            # log.debug("closed tmpfh %s -> %s"% (tmpfh.name, filename))
