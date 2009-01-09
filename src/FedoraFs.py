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

        # FIXME: find a cleaner way to pass username/password for risearch?
        self.ri = risearch.Risearch("http://" + self.username + ":" + self.password + "@"
                                    + self.host + ":" + self.port + "/fedora")
        fuse.Fuse.main(self, args)

    

    def fedoratime(self, datetime):
        # parse fedora format date into date_struct, convert to unix time, then int
        # trimming off .000Z for simplicity
        return int(mktime(strptime(datetime[0:-5], "%Y-%m-%dT%H:%M:%S")))


    def parsepath(self, path):
        # path info to be populated and returned
        info = {'date' : None}		# default to no date, will be set if versioned
        
        if path == '/':
            info['mode'] = 'root'
            return info
        
        path_els = path.split('/')[1:]

        # fedora pid will always be the first element
        info['pid'] = path_els[0]
        # related required at top-level to get number of links for object dir
        info['related'] = self.ri.getObjectRelations(info['pid'])
        depth = len(path_els)

        # first level down is object, e.g. /pid
        if depth == 1:
            info['mode'] = 'object'
            # only information required is pid & related
            # methodlist required for reading directory
            info['methodlist'] =  self.fedora.listMethods_REST(info['pid'])
            return info

        # look for the simpler things first (no fedora lookups required)
        if ".versions" in path:
            if path_els[-1] == ".versions":
                info['mode'] = "version_list"
                return info
            else:
                info['date'] = path_els[2]

            # get versioned methodlist
            info['methodlist'] =  self.fedora.listMethods_REST(info['pid'], info['date'])
                
            if depth == 3:
                # versioned view of the object
                info['mode'] = "object"
                return info
        else:
            # get unversioned methodlist
            info['methodlist'] =  self.fedora.listMethods_REST(info['pid'])
        

        # if last element is .info, mode is object info (may or may not be versioned)
        if path_els[-1] == ".info":
            info['mode'] = 'info'
            return info

        # object component - versioned or unversioned - examples:
        # 	/pid/DC or /pid/.versions/2008-01-01/DC	 - datastream
        # 	/pid/getText or /pid/.versions/2008-01-01/getText  - method (dissemination)
        # 	/pid/hasMember or /pid/.versions/2008-01-01/hasMember  - relation 
        
        if depth == 2 or depth == 4:
            part = path_els[-1]
            
            if part in info['related'].keys():
                info['mode'] = 'relation'
                info['relation'] = part
                return info

            if self.fedora.doesDatastreamExist_REST(info['pid'], part):
                info['mode'] = 'datastream'
                info['dsid'] = part
                return info

            methods = []
            if len(info['methodlist']):
                for bdef in info['methodlist'].keys():
                    for method in info['methodlist'][bdef]:
                        methods.append(method.encode('ascii'))
            if part in methods:
                info['mode'] = 'method'
                info['method'] = part
                return info

        # add related object  
        if depth == 3:		# is this test sufficient ? actually check relation?
            info['mode'] = "related_object"
            info['rel_pid'] = path_els[-1]
            
        # probably shouldn't fall through to here if this is working properly... 
        return info
        

    def getattr(self, path):
        path_info = self.parsepath(path)

        st = MyStat()	 # access time defaults to now


        # if we are in a versioned view of the object, we know what the modified time should be...
        if path_info['date']:
            st.st_mtime = self.fedoratime(path_info['date'])

        # top-level directory
        if path_info['mode'] == "root":
            st.st_mode = stat.S_IFDIR | 0755
            # for a directory, number of links should be subdirs + 2
            # make sure pid list is up-to-date before calculating
            self.getpids()
            st.st_nlink = 2 + len(self.pids)
            
        elif path_info['mode'] == "object":
            # NOTE: this handles both versioned and unversioned object view (mostly)
            st.st_mode = stat.S_IFDIR | 0755
            # currently, only possible subdirs for an object are relations to other objects & .versions
            # neither of these are applicable for a versioned view
            if path_info['date'] == None:
                st.st_nlink = 2 + len(path_info['related'].keys()) + 1
            else:
                st.st_nlink = 2

            # getting versioned view of object profile  (doesn't work properly in fedora2.2 ?)
            profile = self.fedora.getObjectProfile(path_info['pid'], "dom", path_info['date'])
            if profile:
                st.st_ctime = self.fedoratime(profile['objCreateDate'])
                # only use last modified date from profile in unversioned view
                if path_info['date'] == None:
                    st.st_mtime = self.fedoratime(profile['objLastModDate'])

        elif path_info['mode'] == "info":
            st.st_mode = stat.S_IFREG | 0444
            st.st_size = len(self.info(path_info['pid'], path_info['date']))

        elif path_info['mode'] == "version_list":
            # directory of revisions in the object's history
            st.st_mode = stat.S_IFDIR | 0755
            history = self.fedora.getObjectHistory(path_info['pid'])
            st.st_nlink = 2 + len(history)

        elif path_info['mode'] == "datastream":
            ## NOTE: if datastream exist but account does not have access,
            ## it will be displayed as a zero-size file
            
            # display as a regular file
            st.st_mode = stat.S_IFREG | 0444
            st.st_nlink = 1
            ## FIXME: datastream creation/modification time?

            # use API-M to get datastream info
            dsprofile = self.fedora.getDatastreamProfile(path_info['pid'], path_info['dsid'], path_info['date'])
            if dsprofile:
                st.st_size = dsprofile._datastream._size
                st.st_ctime = self.fedoratime(dsprofile._datastream._createDate.encode('ascii'))
                # FIXME: does not return mtime;  use object mtime?
                # ARGH: fedora apparently returns 0 size for managed datastreams ?
                
            if not(dsprofile) or st.st_size == 0:
                # as fall-back only, get size from datastream itself
                # Note that this very slow (& possibly inaccurate) for large datastreams
                content = self.fedora.getDatastream(path_info['pid'], path_info['dsid'], path_info['date'])
                st.st_size = len(content)
            
        elif path_info['mode'] == "method":
            # display as a regular file
            st.st_mode = stat.S_IFREG | 0444
            st.st_nlink = 1
            ## FIXME: dissemination creation/modification time?
            ## FIXME2: this will be *really* slow (& inaccurate) for large datastreams...
            ## (figure out how to enable fuse caching ... ?)
            #def getDissemination_REST(self, pid, bdef, method):
            
            for bdefpid in path_info['methodlist'].keys():
                if path_info['method'] in path_info['methodlist'][bdefpid]:
                    bdef = bdefpid
            content = self.fedora.getDissemination_REST(path_info['pid'], bdef, path_info['method'],
                                                        path_info['date'])
            st.st_size = len(content)

        elif path_info['mode'] == "relation":
            # relation - treat as a directory containing other objects
            st.st_mode = stat.S_IFDIR | 0755
            # for a directory, # of links should be subdirs + 2
            # count the number of objects related by the specified relationship
            st.st_nlink = 2   + len(path_info['related'][path_info['relation']])

        elif path_info['mode'] == "related_object":
            st.st_mode = stat.S_IFLNK | 0755

        else:
            # no such file or directory
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
        
    
    def info(self, pid, date=None):
    # generate .info file contents based on what is in the object profile
        profile = self.fedora.getObjectProfile(pid, "dom", date)
        lines = ["object info for " + pid + "\n\n"]
        for info in profile:
            lines.append(info + ": " + profile[info] + '\n' )
        return ''.join(lines).encode('ascii')


    def readdir (self, path, offset):
        dirents = [ '.', '..' ]

        ## FIXME: howto use offset for large directories ?

        path_info = self.parsepath(path)

        if path_info['mode'] == 'root':
            self.getpids()	# make sure pid list is populated
            dirents.extend(self.pids)
            
        elif path_info['mode'] == "object":
            # NOTE: this handles both versioned and unversioned object view (mostly)
            # pid is a directory containing datastreams as files

            # if this pid was not in the list yet for some reason, add it  
            if path_info['pid'] not in self.pids:
                self.pids.append(path_info['pid'])

            dirents.append(".info")
            # if we are not in a versioned view of the object, add .version dir
            if path_info['date'] == None:
                dirents.append(".versions")
                
            dslist = self.fedora.listDatastreams_REST(path_info['pid'], path_info['date'])
            # convert unicode datastream names to ascii
            for ds in dslist.keys():
                dirents.append(ds.encode('ascii'))
                
            if len(path_info['methodlist']):
                for bdef in path_info['methodlist'].keys():
                    for method in path_info['methodlist'][bdef]:
                        dirents.append(method.encode('ascii'))

            # relations to other objects - *only* present in unversioned view (risearch not versioned)
            if path_info['date'] == None:
                for relation in path_info['related'].keys():
                    dirents.append(relation)

        elif path_info['mode'] == "version_list":
            # .versions subdir
            dirents.extend(self.fedora.getObjectHistory(path_info['pid']))

        elif path_info['mode'] == "relation":
            # relation subdir
            for obj in path_info['related'][path_info['relation']]:
                dirents.append(obj)
                
        #else:
            # FIXME: any other cases? shouldn't fall down to here...
            
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
        path_info = self.parsepath(path)

        str = ""
        
        if path_info['mode'] == "info":
            # get object info (may be versioned)
            str = self.info(path_info['pid'], path_info['date'])

        if path_info['mode'] == "datastream":
            str = self.fedora.getDatastream(path_info['pid'], path_info['dsid'], path_info['date'])

        if path_info['mode'] == "method":
            for bdefpid in path_info['methodlist'].keys():
                if path_info['method'] in path_info['methodlist'][bdefpid]:
                    bdef = bdefpid
            str = self.fedora.getDissemination_REST(path_info['pid'], bdef, path_info['method'],
                                                    path_info['date'])

            
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
#        pe = path.split('/')[1:]        # Path elements 0 = printer 1 = file
#        self.files[pe[1]] += buf
#        return len(buf)
	return 0

    ## FIXME: implementation? what needs to be done here?
    def release(self, path, flags):
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
