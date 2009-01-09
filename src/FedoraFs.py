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

from pythonFedoraCommons import fedoraClient,risearch

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
        # fedora-specific configuration parameters
	# these are the defaults; can be overridden with command-line options
        self.host = "localhost"
        self.port = "8080"
        self.username = "fedoraAdmin"
        self.password = "fedoraAdmin"
        self.version  = "2.2"
        
        fuse.Fuse.__init__(self, *args, **kw)

## wilson fedora22 pids (find not working for some reason)
        self.testpids =  ["emory:8083", "emory:8096", "emory:80b9", "emory:bvrb", "emory:8g4n",
#                         "emory:8hfn",	## contains LARGE files & is very slow
                          "emory:8h84"]
        self.pids = []
	self.files = {}
        self.lastfiles = {}

    def main(self, args=None):
        # initialize fedora connection AFTER command line options have been parsed
        client = fedoraClient.ClientFactory()
        self.fedora = client.getClient("http://" + self.host + ":" + self.port + "/fedora",
                                       self.username, self.password, self.version)
        
        self.ri = risearch.Risearch("http://" + self.host + ":" + self.port + "/fedora")
        fuse.Fuse.main(self, args)

    

    def fedoratime(self, datetime):
        # parse fedora format date into date_struct, convert to unix time, then int
        # trimming off .000Z for simplicity
        return int(mktime(strptime(datetime[0:-5], "%Y-%m-%dT%H:%M:%S")))

    def getattr(self, path):
        pe = path.split('/')[1:]

        st = MyStat()
        # access time defaults to now 

        ## FIXME: add some generic parse/path stuff here (could also be used in read func)

        if len(pe):
            pid = pe[0]
            

        
        if path == '/':
            st.st_mode = stat.S_IFDIR | 0755
            # for a directory, number of links should be subdirs + 2
            # make sure pid list is up-to-date before calculating
            self.getpids()
            st.st_nlink = 2 + len(self.pids)

        elif len(pe) == 1:
            # first level down is pid (1 path element, /pid)
            st.st_mode = stat.S_IFDIR | 0755
            # currently, only possible subdirs for an object are relations to other objects & .versions
            related = self.ri.getObjectRelations(pid)
            st.st_nlink = 2 + len(related.keys()) + 1

            profile = self.fedora.getObjectProfile(pid, "dom")
            if profile:
                st.st_ctime = self.fedoratime(profile['objCreateDate'])
                st.st_mtime = self.fedoratime(profile['objLastModDate'])



        elif len(pe) == 2:
            # second level could be one of:
            #  	datastream 		e.g., /pid/DC
            #   top-level info  	      /pid/.info
            # 	dissemination		      /pid/getText
            # 	relation (dir)		      /pid/hasMember

            methods = []
            methodlist = self.fedora.listMethods_REST(pid)
            if len(methodlist):
                for bdef in methodlist.keys():
                    for method in methodlist[bdef]:
                        methods.append(method.encode('ascii'))

            related = self.ri.getObjectRelations(pid)
#            for relation in related.keys():
#                dirents.append(relation)
                
                    

            if pe[1] == ".info":
                st.st_mode = stat.S_IFREG | 0444
                st.st_size = len(self.info(pid))
                
            elif pe[1] == ".versions":
                st.st_mode = stat.S_IFDIR | 0755
                history = self.fedora.getObjectHistory(pid)
                st.st_nlink = 2 + len(history)
                
            elif self.fedora.doesDatastreamExist_REST(pid, pe[1]):
                ## NOTE: if datastream exist but account does not have access,
                ## it will be displayed as a zero-size file
                
                # display as a regular file
                st.st_mode = stat.S_IFREG | 0444
                st.st_nlink = 1
                ## FIXME: datastream creation/modification time?
                ## FIXME2: this is *really* slow (& inaccurate) for large datastreams...
                
                content = self.fedora.getDatastream(pid, pe[1])
                st.st_size = len(content)
            elif pe[1] in methods:
                method = pe[1]
                # display as a regular file
                st.st_mode = stat.S_IFREG | 0444
                st.st_nlink = 1
                ## FIXME: dissemination creation/modification time?
                ## FIXME2: this will be *really* slow (& inaccurate) for large datastreams...
                ## (figure out how to enable fuse caching ... ?)
                #def getDissemination_REST(self, pid, bdef, method):

                for bdefpid in methodlist.keys():
                    if method in methodlist[bdefpid]:
                        bdef = bdefpid
                content = self.fedora.getDissemination_REST(pid, bdef, method)
                st.st_size = len(content)
                
            elif pe[1] in related.keys():
                # relation - treat as a directory containing other objects
                st.st_mode = stat.S_IFDIR | 0755
                # for a directory, # of links should be subdirs + 2
            	st.st_nlink = 2	+ len(related[pe[1]])

            else:
                # no such file or directory
                return -errno.ENOENT
            
        elif len(pe) == 3:
            # third level item -- related object, e.g. pid/hasMember/relpid
            # OR version changetime, e.g. pid/.versions/2008-11-01
            if pe[1] == ".versions":
                # treat as a directory
                st.st_mode = stat.S_IFDIR | 0755
                # how to determine nlink count ?
            
            else:
                st.st_mode = stat.S_IFLNK | 0755

        elif len(pe) == 4:
            # fourth level item -- versioned part, e.g. pid/.versions/2008-11-01/DC

            # FIXME: extend doesDatastreamExist function to use datetime stamp, check here

            # display as a regular file
            st.st_mode = stat.S_IFREG | 0444
            st.st_nlink = 1
            ## FIXME: datastream creation/modification time?
            ## FIXME2: this is *really* slow (& inaccurate) for large datastreams...

            st.st_mtime = self.fedoratime(pe[-2])
            content = self.fedora.getDatastream(pid, pe[-1], pe[-2])	# dsid, datetime
            st.st_size = len(content)
            
        else:
            return -errno.ENOENT
        
        return st


    def getpids(self):
        # initialize list of pids for top-level directory, if not already done
        if len(self.pids) == 0:
            # only search if pid list has not already been populated
            pids = self.fedora.findObjects_REST("*")
            if len(pids):
                self.pids = pids
            else:
                # fallback list of pids for testing (find not working)
                self.pids = self.testpids
        
    
    def info(self, pid):
    # generate .info file contents based on what is in the object profile
        profile = self.fedora.getObjectProfile(pid, "dom")
        lines = ["object info for " + pid + "\n\n"]
        for info in profile:
            lines.append(info + ": " + profile[info] + '\n' )
        return ''.join(lines).encode('ascii')


    def readdir (self, path, offset):
        dirents = [ '.', '..' ]

        ## FIXME: howto use offset for large directories ?
        
        pe = path.split('/')[1:]
        if path == '/':
            self.getpids()	# make sure pid list is populated
            dirents.extend(self.pids)
        elif len(pe) == 1:
            # pid is a directory containing datastreams as files
            pid = pe[0]

            # if this pid was not in the list yet for some reason, add it  
            if pid not in self.pids:
                print "*** pid " + pid + " is not in self.pids, appending\n"
                self.pids.append(pid)

            dirents.append(".info")
            dirents.append(".versions")
            dslist = self.fedora.listDatastreams_REST(pid)
            # convert unicode datastream names to ascii
            for ds in dslist.keys():
                dirents.append(ds.encode('ascii'))
                
            methodlist = self.fedora.listMethods_REST(pid)
            if len(methodlist):
                for bdef in methodlist.keys():
                    for method in methodlist[bdef]:
                        dirents.append(method.encode('ascii'))

            # relations to other objects
            related = self.ri.getObjectRelations(pid)
            for relation in related.keys():
                dirents.append(relation)
                
        elif len(pe) == 2:		# relation subdir OR .versions subdir
            pid = pe[0]

            if pe[1] == ".versions":
                history = self.fedora.getObjectHistory(pid)
                dirents.extend(history)
                
            else:
                related = self.ri.getObjectRelations(pid)
                for obj in related[pe[1]]:
                    dirents.append(obj)

        elif len(pe) == 3:		# .versions/date subdir
            pid = pe[0]
            date = pe[2]

            dslist = self.fedora.listDatastreams_REST(pid, date)
            # convert unicode datastream names to ascii
            for ds in dslist.keys():
                dirents.append(ds.encode('ascii'))

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


        if len(pe) == 4:
            # fourth level item -- versioned part, e.g. pid/.versions/2008-11-01/DC
            # FIXME: extend doesDatastreamExist function to use datetime stamp, check here (?)
            str = self.fedora.getDatastream(pid, pe[-1], pe[-2])	# dsid, datetime

        else:
            methods = []
            methodlist = self.fedora.listMethods_REST(pid)
            if len(methodlist):
                for bdef in methodlist.keys():
                    for method in methodlist[bdef]:
                        methods.append(method.encode('ascii'))

            if pe[1] == ".info":
                str = self.info(pid)
            elif self.fedora.doesDatastreamExist_REST(pid, pe[1]):
                str = self.fedora.getDatastream(pid, pe[1])
            elif pe[1] in methods:
                method = pe[1]
                for bdefpid in methodlist.keys():
                    if method in methodlist[bdefpid]:
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

    def readlink(self, path):
        # for now, the only symlinks supported are relation/pid
        pe = path.split('/')
        pid = pe[-1]	#  last element is pid - link to top-level pid entry
        newpath = "../../" + pid
        return newpath

    
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

    def fuseoptref(self):
        fuse_args = fuse.FuseArgs()
        fuse.args.optlist = {"host" : self.host,
                             "port" : self.port,
                             "username" : self.username,
                             "password" : self.password,
                             "version" : self.version}
        return fuse_args

  

def main():
    usage="""
    FedoraFS: A filesystem to access content in a Fedora repository.
    """ + fuse.Fuse.fusage
    
    server = FedoraFS(version="%prog " + fuse.__version__,
                      usage=usage, dash_s_do='setsingle')

    # fedora-specific mount options
    ## FIXME: could maybe use add_option_group function?
    server.parser.add_option(mountopt="host", metavar="HOSTNAME", default=server.host,
                             help="fedora server host name [default: %default]")
    server.parser.add_option(mountopt="port", metavar="PORT", default=server.port,
                             help="fedora server port number [default: %default]")
    server.parser.add_option(mountopt="username", metavar="USER", default=server.username,
                             help="fedora user [default: %default]")
    server.parser.add_option(mountopt="password", metavar="PASSWORD", default=server.password,
                             help="fedora password [default: %default]")
    server.parser.add_option(mountopt="version", metavar="VERSION", default=server.version,
                             help="version of fedora (2.2, 3.0) [default: %default]")
    server.parse(values=server, errex=1)
    server.main()
    
if __name__ == '__main__':
    main()
