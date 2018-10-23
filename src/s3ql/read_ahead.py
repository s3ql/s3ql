# -*- coding:<utf-8> -*-

'''
read_ahead.py - this file is part of S3QL.

Copyright  2018 Tommaso Massimi <t.massim@cynnyspace.com>

This work can be distributed under the terms of the GNU GPLv3.
'''


import threading, time, os, shutil

if not __name__ == "__main__":
  from .logging import logging # Ensure use of custom logger class
  from . import BUFSIZE
  from .backends.common import NoSuchObject
  # standard logger for this module
  log = logging.getLogger(__name__)

else:
  # DEBUGGG
  BUFSIZE = 100 #64*1024
  NoSuchObject=1
  class logging():
    def debug(a,*args):
      print(args[0])
  
  log = logging()


class ReadAhead_details():
    '''
    keeps read_ahead variables for each file
    '''
    def __init__(self, dict_block_no_objid, blockno):
        self.last_block_read = blockno
        self.consec_block_count = 1
        self.RAlast_access = time.time()

        self.dict_block_no_objid = dict_block_no_objid
        try:
            del self.dict_block_no_objid[blockno]        # remove this block from the list of blocks to be downloaded 
        except:
            pass

        self.block_count_trigger = 2 # parameter

    def Required(self, blockno):
        '''
        keeps the number of consecutive block read.
        '''
        #log.debug("Required start blockno=%d\n\tconsec_block_count=%d\n\tlast_blockno_read=%d" % (blockno, self.consec_block_count, self.last_block_read))
        
        self.RAlast_access = time.time()
        # TODO remove already required blockno from dict_block_no_objid.remove(blockno)

        if self.last_block_read +1 == blockno:
            self.consec_block_count += 1
            log.debug("Required self.consec_block_count=%d, self.last_block_read=%d" % (self.consec_block_count, self.last_block_read))
        elif self.last_block_read == blockno:
            pass
        else:
            self.consec_block_count = 0
            log.debug("Required self.consec_block_count=%d, self.last_block_read=%d" % (self.consec_block_count, self.last_block_read))
        self.last_block_read=blockno
        
        if blockno in self.dict_block_no_objid.keys(): # this block will not be downloaded in read_ahead mode
            del self.dict_block_no_objid[blockno]
        log.debug("Required end blockno=%d\n\tconsec_block_count=%d\n\tlast_blockno_read=%d\n" % (blockno, self.consec_block_count, self.last_block_read))

    def TriggerOn(self):
        '''
        rule declaring when some file block can be downloaded
        '''
        return self.consec_block_count >= self.block_count_trigger
      
    def Expired(self):
      if time.time() - self.RAlast_access > 40:
          return True
      return False

      
# TODO remove data related to old files


#class ThreadVars():
#    def __init__(self, inode, blockno, fn):
#        self.inode = inode
#        self.blockno = blockno
#        self.filename = fn
#        self.t = None
 
class ReadAhead(object):
    '''
    Read Ahead block logic
    '''

    def __init__(self):
        self.ReadAheadData = dict()           # map inode -> ReadAhead_details
        #self.async_download_threads=[]
        self.nRAThread = 25 # param, nMax Threads
        self.threadLock = threading.Lock()
        self.listObjIDDwnInProgress = []

    def BlockDownloadRequired(self, path, inode, blockno, obj_id, backend_pool):
        log.debug("BlockDownloadRequired: inode=%d, blockno=%d, obj_id=%d, keys=%s" % (inode, blockno, obj_id, self.ReadAheadData.keys()))
        if inode in self.ReadAheadData.keys():
            self.ReadAheadData[inode].Required(blockno)
        else:
            log.debug("ahead error!!! new BlockDownloadRequired: inode=%d, blockno=%d, obj_id=%d, keys=%s" % (inode, blockno, obj_id, self.ReadAheadData.keys()))
            #self.ReadAheadData[inode] = ReadAhead_details(blockno)
            #log.debug("BlockRequired: added key inode=%d, blockno=%d, obj_id=%d, curr keys=%s" % (inode, blockno, obj_id, self.ReadAheadData.keys()))
        #filename = os.path.join(path, '%d-%d' % (inode, blockno))

        #log.debug("BlockDownloadRequired: dopo inode=%d, blockno=%d, obj_id=%d, keys=%s" % (inode, blockno, obj_id, self.ReadAheadData.keys()))

        if self.ReadAheadData[inode].TriggerOn():
            log.debug ("Trigger on, list down in progr=%s"% self.listObjIDDwnInProgress)
            if obj_id not in self.listObjIDDwnInProgress:
              # filename = os.path.join(path, '%d-%d' % (inode, blockno))
              self.StartThreads(obj_id, inode, blockno, backend_pool, path)
              return True; # thread started 
        return False # thread not started

    def StartThreads(self, obj_id, inode, blockno, backend_pool, path):
        list_objid = []
        list_bn = []

        # choose the objId to downlaod asynchrously
        log.debug("RAhead thread lanciabili: %s" % self.ReadAheadData[inode].dict_block_no_objid.keys())
        for bn in self.ReadAheadData[inode].dict_block_no_objid.keys():
            list_bn.append(bn)
            list_objid.append((bn, self.ReadAheadData[inode].dict_block_no_objid[bn]))
            if len(list_bn) >= self.nRAThread:
                break

        # remove the objId from the list to avoid a re-downlaod
        for bn in list_bn:
            del self.ReadAheadData[inode].dict_block_no_objid[bn] # remove these items from the dict, so it will not be downloaded again

        thList=[]
        # create threads
        for (bn, objId) in list_objid:
            #log.debug("creating RAhead thread inode=%d, objid=%d" % (inode, objId))
            self.listObjIDDwnInProgress.append( objId )
            #filename = os.path.join(path, '%d-%d' % (inode, bn))
            #tv = ThreadVars(inode, blockno, filename)
            #tv.t = RAThread(target=self.getBlock, args=(objId, inode, backend_pool,path) )
            t = threading.Thread(target=self.getBlock, args=(bn, objId, inode, backend_pool,path) )
            
            #toma t.setValues(inode, blockno, filename)
            #self.async_download_threads.append(t)
            #self.thList.append(tv)
            thList.append(t)
            #log.debug("created  RAhead thread inode=%d, objid=%d" % (inode, objId))
            
        #log.debug("End RAhead creation")

        # start threads
        for thvar in thList:
           thvar.start()
           #thvar.t.start()
        log.debug("RAhead objID thread lanciati: %s" % self.listObjIDDwnInProgress)
        for thvar in thList:
          thvar.join()
        log.debug("fine join")

    def __nonusato_CheckEndedThreads(self):
        """ 
        if a thread is enend, return some values to add the fileblock cache
        """
        rv =tuple
        for thvar in self.thList:
          thvar.t.join(0.01)
          if not thvar.t.isAlive():
              (i,b,n) = (thvar.inode, thvar.tblockno, thvar.tfilename)
              self.thList.remove(thvar)
              rv.insert((i,b,n),)
        return rv

    def getBlock(self,bn, obj_id, inode, backend_pool,path):
        filename = os.path.join(path, '%d-%d' % (inode, bn))
        tmpfh = open(filename + '.async_tmp', 'wb')
        log.debug("opened tmpfh %s -> %s"% (tmpfh.name, filename))
        try:
            def do_read(fh):
                tmpfh.seek(0)
                tmpfh.truncate()
                shutil.copyfileobj(fh, tmpfh, BUFSIZE)
        
            log.debug("start RAhead thread inode=%d, blockNo=%d, obj_id=%d, " % (inode, bn, obj_id))

            if not __name__ == "__main__":
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
            else:
              # DEBUGGG
              fn = filename + '.async_tmp'
              f=open(fn,"wb+")
              f.seek(100+obj_id)
              f.write(b"\0")
              shutil.copyfileobj(f, tmpfh, BUFSIZE)
              f.close()
              
            os.rename(tmpfh.name, filename)
            log.debug("renamed %s -> %s"% (tmpfh.name, filename)) 
        except:
            os.unlink(tmpfh.name)
            raise
        finally:
            log.debug("closing tmpfh %s -> %s"% (tmpfh.name, filename))
            tmpfh.close()
            log.debug("closed tmpfh %s -> %s"% (tmpfh.name, filename))


if __name__ == "__main__":
    log.debug("start")
    ra = ReadAhead()
    fn='pippo'
    nTotBlocks=10
    inode=1
    backend_pool=None
    path="."
    dict_blockno_objid={0:0,1:11,2:22,3:33,4:44,5:55,6:66,7:77,8:88,9:99}
    
    for block_nr in range(0,nTotBlocks):
        if inode not in ra.ReadAheadData.keys():
          ra.ReadAheadData[inode] = ReadAhead_details(dict_blockno_objid, block_nr)
        obj_id = block_nr + 1
        #if block_nr == 3:
            #ra.BlockRequired(fn,6,nTotBlocks) # tests the block counter
        #    ra.BlockDownloadRequired(path, inode, block_nr, obj_id, backend_pool)


        if ra.BlockDownloadRequired(path, inode, block_nr, obj_id, backend_pool):
        #if ra.BlockRequired(fn,block_nr,nTotBlocks):
            log.debug ("Trigger on for block %d" % block_nr)
            #ra.StartThreads(fn,block_nr)
    print("end")
    
