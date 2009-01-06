#!/usr/bin/python

from fuse import Fuse

from time import time

import stat    # for file properties
import os      # for filesystem modes (O_RDONLY, etc)
import errno   # for error number codes (ENOENT, etc)
               # - note: these must be returned as negatives
import fuse
#from time import time
from time import *	# workaround for strptime
from subprocess import *

from pythonFedoraCommons import fedoraClient

fuse.fuse_python_api = (0, 2)

class MyStat(fuse.Stat):
    def __init__(self):
        self.st_mode = stat.S_IFDIR | 0755
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 2
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 4096
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0


class FedoraFS(fuse.Fuse):
    def __init__(self, *args, **kw):
        fuse.Fuse.__init__(self, *args, **kw)

        self.pids = {"emory:8083" : "emory:8083",
                     "emory:8096" : "emory:8096",
                     "emory:80b9" : "emory:80b9",
                     "emory:bvrb" : "emory:bvrb",
                     "emory:8g4n" : "emory:8g4n",
#                     "emory:8hfn" : "emory:8hfh",	## LARGE
                     "emory:8h84" : "emory:8h84",
                     }
        self.files = {}
        self.lastfiles = {}
        client = fedoraClient.ClientFactory()
        self.fedora = client.getClient("http://wilson:6080/fedora", "fedoraAdmin", "fedoraAdmin", "2.2")

    def fedoratime(self, datetime):
        # parse fedora format date into date_struct, convert to unix time, then int
        # trimming off .000Z for simplicity
        return int(mktime(strptime(datetime[0:-5], "%Y-%m-%dT%H:%M:%S")))

    def getattr(self, path):
        pe = path.split('/')[1:]
        #st.st_mtime = st.st_atime
        #st.st_ctime = st.st_atime

        st = MyStat()
        # access time is now 
#        st.st_atime = int(time())
        
        if path == '/':
            st.st_mode = stat.S_IFDIR | 0755
            st.st_nlink = 2

        elif len(pe) == 1:
            # first level down is pid (1 path element, /pid)
            st.st_mode = stat.S_IFDIR | 0755
            #st.st_nlink = 2	# ?
            profile = self.fedora.getObjectProfile(pe[0], "dom")
            st.st_ctime = self.fedoratime(profile['objCreateDate'])
            st.st_mtime = self.fedoratime(profile['objLastModDate'])

        elif len(pe) == 2:
            pid = pe[0]
            # second level could be one of:
            #  	datastream 		e.g., /pid/DC
            #   top-level info  	      /pid/.info
            # 	dissemination		      /pid/getText

            methods = []
            methodlist = self.fedora.listMethods_REST(pid)
            for bdef in methodlist.keys():
                for method in methodlist[bdef]:
                    methods.append(method.encode('ascii'))

            if pe[1] == ".info":
                st.st_mode = stat.S_IFREG | 0444
                st.st_nlink = 1
                
                profile = self.fedora.getObjectProfile(pid, "dom")
                str = "object info for " + pid
                lines = [str]
                for info in profile:
                    lines.append(info + ": " + profile[info] + '\n' )
                content = ''.join(lines)

                st.st_size = len(content)
            ## FIXME: this function seems to *always* return true... ?  (hacked..)
            elif self.fedora.doesDatastreamExist_REST(pid, pe[1]):
                # display as a regular file
                st.st_mode = stat.S_IFREG | 0444
                st.st_nlink = 1
                ## FIXME: datastream creation/modification time?
                ## FIXME2: this is *really* slow (& inaccurate) for large datastreams...
                
                content = self.fedora.getDatastream(pid, pe[1])
                st.st_size = len(content)
            elif methods.__contains__(pe[1]):
                method = pe[1]
                # display as a regular file
                st.st_mode = stat.S_IFREG | 0444
                st.st_nlink = 1
                ## FIXME: dissemination creation/modification time?
                ## FIXME2: this will be *really* slow (& inaccurate) for large datastreams...
                #def getDissemination_REST(self, pid, bdef, method):

                for bdefpid in methodlist.keys():
                    if methodlist[bdefpid].__contains__(method):
                        bdef = bdefpid
                content = self.fedora.getDissemination_REST(pid, bdef, method)
                st.st_size = len(content)
            else:
                # no such file or directory
                return -errno.ENOENT

        else:
            
            profile = self.fedora.getObjectProfile(pe[0], "dom")
            st.st_ctime = self.fedoratime(profile['objCreateDate'])
            st.st_mtime = self.fedoratime(profile['objLastModDate'])
             
            # treat as regular file?
            st.st_mode = stat.S_IFREG | 0444
            
            st.st_nlink = 1
            #st.st_size = 0

            #ds = self.fedora.listDatastreams_REST(pe[0])
            #buf = ""
            #for d in ds:
            #    buf += d + "\n"
            ds = self.fedora.getListDatastreams(pe[0])
            self.files[pe[0]] = ds
            
            st.st_size = len(ds)

            

            #str = 'Hello World!\n'
            #st.st_size = len(str)

            #return -errno.ENOENT
        return st



    def readdir (self, path, offset):
        dirents = [ '.', '..' ]
        
        pe = path.split('/')[1:]
        if path == '/':
            dirents.extend(self.pids.keys())
        elif len(pe) == 1:
            pid = pe[0]
            dirents.append(".info")
            # pid is a directory containing datastreams as files
            dslist = self.fedora.listDatastreams_REST(pid)
            # convert unicode datastream names to ascii
            for ds in dslist.keys():
                dirents.append(ds.encode('ascii'))
                
            methodlist = self.fedora.listMethods_REST(pid)
            for bdef in methodlist.keys():
                for method in methodlist[bdef]:
                    dirents.append(method.encode('ascii'))
        else:
            # Note use of path[1:] to strip the leading '/'
            # from the path, so we just get the printer name
            dirents.extend(self.pids[path[1:]])
            
        for r in dirents:
            yield fuse.Direntry(r)

    def mknod(self, path, mode, dev):
        pe = path.split('/')[1:]        # Path elements 0 = printer 1 = file
        self.pids[pe[0]].append(pe[1])
        self.files[pe[1]] = ""
        self.lastfiles[pe[1]] = ""
        return 0
    
    def unlink(self, path):
        pe = path.split('/')[1:]        # Path elements 0 = printer 1 = file
        return 0
    
    def read(self, path, size, offset):
        pe = path.split('/')[1:]        # Path elements 0 = pid 1 = dsid
        pid = pe[0]
        dsid = pe[1]


        methods = []
        methodlist = self.fedora.listMethods_REST(pid)
        for bdef in methodlist.keys():
            for method in methodlist[bdef]:
                methods.append(method.encode('ascii'))

        if pe[1] == ".info":
            profile = self.fedora.getObjectProfile(pid, "dom")
            lines = ["object info for " + pid + "\n\n"]
            for info in profile:
                lines.append(info + ": " + profile[info] + '\n' )
            str = ''.join(lines).encode('ascii')
        elif self.fedora.doesDatastreamExist_REST(pid, pe[1]):
            str = self.fedora.getDatastream(pid, pe[1])
        elif methods.__contains__(pe[1]):
            method = pe[1]
            for bdefpid in methodlist.keys():
                if methodlist[bdefpid].__contains__(method):
                    bdef = bdefpid
            str = self.fedora.getDissemination_REST(pid, bdef, method)

        slen = len(str)
        if offset < slen:
            if offset + size > slen:
               size = slen - offset
            buf = str[offset:offset+size]
        else:
            buf = ''
        return buf

        
        # 0 = pid
        #ds = self.fedora.listDatastreams_REST(pe[0])
        #self.lastfiles[pe[0]] = ""
        #for d in ds:
        #    self.lastfiles[pe[0]] += d
        #return contents[offset:offset+size]
        #return self.lastfiles[pe[0]][offset:offset+size]
    
    def write(self, path, buf, offset):
        pe = path.split('/')[1:]        # Path elements 0 = printer 1 = file
        self.files[pe[1]] += buf
        return len(buf)
    
    def release(self, path, flags):
        pe = path.split('/')[1:]        # Path elements 0 = printer 1 = file
        if len(self.files[pe[1]]) > 0:
            lpr = Popen(['lpr -P ' + pe[0]], shell=True, stdin=PIPE)
            lpr.communicate(input=self.files[pe[1]])
            lpr.wait()
            self.lastfiles[pe[1]] = self.files[pe[1]]
            self.files[pe[1]] = ""      # Clear out string
        return 0
           
    def open(self, path, flags):
        return 0
    
    def truncate(self, path, size):
        return 0
    
    def utime(self, path, times):
        return 0
    
    def mkdir(self, path, mode):
        return 0
    
    def rmdir(self, path):
        return 0
    
    def rename(self, pathfrom, pathto):
        return 0
    
    def fsync(self, path, isfsyncfile):
        return 0
  



def main():
    usage="""
    FedoraFS: A filesystem to access content in a Fedora repository.
    """ + fuse.Fuse.fusage
    
    server = FedoraFS(version="%prog " + fuse.__version__,
                      usage=usage, dash_s_do='setsingle')
    server.parse(errex=1)
    server.main()
    
if __name__ == '__main__':
    main()
