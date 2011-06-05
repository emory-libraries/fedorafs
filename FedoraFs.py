#!/usr/bin/env python

# file FedoraFS.py
# 
#   Copyright 2009,2011 Emory University General Library
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.


#import fuse
import argparse
from getpass import getpass
import stat    
import errno
import logging               
from UserString import MutableString

from eulfedora.server import Repository

from models import FsObject, FsStat

from fuse import FUSE, Operations, LoggingMixIn

# not in pypi version? FuseOSError

# simplistic logging for development purposes
LOGGING_LEVEL=logging.DEBUG     # NOLOG, CRITICAL, ERROR, WARNING, INFO, DEBUG
LOGGING_FORMAT="%(levelname)s : %(message)s"
LOGGING_FILENAME='' # STDOUT
logging.basicConfig(level=LOGGING_LEVEL,
        format=LOGGING_FORMAT, filename=LOGGING_FILENAME)
logger = logging.getLogger(__name__)



class FedoraFS(LoggingMixIn, Operations):
    def __init__(self, base_url='http://localhost:8080/fedora/',
                 username='fedoraAdmin', password='fedoraAdmin',
                 filter=None):
        # fedora-specific configuration parameters
	# these are the defaults; can be overridden with command-line options

        # eulcore.fedora.sever requries base_url with a trailing slash
        if not base_url.endswith('/'):
            base_url = '%s/' % base_url

        self.base_url = base_url
        self.username = username
        self.password = password
        self.filter = filter
        print "DEBUG: filter is ", filter

        self.towrite = {}

        self._members = None
        self.files = {}
        self.lastfiles = {}
        self.objects = {}

        # TODO: support netrc for credentials?
        # TODO: catch invalid credentials!! (don't retry if invalid)

        # initialize connection to the repository
        self.repo = Repository(self.base_url, self.username, self.password)
        self.repo.default_object_type = FsObject

    @property
    def members(self):
        if self._members is None:
            # initialize list of members for top-level directory
            # only search if pid list has not already been populated
            found = self.repo.find_objects(terms=self.filter)
            if found:
                self._members = {}
                for i in range(150):     #  ??? how to limit this reasonably?
                #for i in range(10):     #  ??? how to limit this reasonably?
                    obj = found.next()
                    self._members[obj.pid] = obj
        return self._members

    def getattr(self, path, fh=None):
        path_els = path.strip('/').split('/')
        logger.debug('getattr for path %s - path elements are %s' % (path, path_els))
        st = FsStat()	 # access time defaults to now

        if path == '/':
            st.st_mode = stat.S_IFDIR | 0755
            # for a directory, number of links should be subdirs + 2
            # make sure pid list is up-to-date before calculating
            logger.debug('members are %s, count is %s' % (self.members, len(self.members.keys())))
            st.st_nlink = 2 + len(self.members.keys())
            return st.as_dict()
        else:
            if path_els[0] in self.members:
                return self.members[path_els[0]].fs_attr(*path_els[1:])
            else:
                return {}     # ??
            #st.st_mode = stat.S_IFREG | 0444
        return st.as_dict()

    def readdir (self, path, offset):
        dir_entries = [ '.', '..' ]

        # FIXME: howto use offset for large directories ?        
        path_els = path.strip('/').split('/')
        logger.debug('readdir for path %s, offset %s - path elements are %s' % \
                    (path, offset, path_els))

        if path == '/':        # if root 
            dir_entries.extend([obj.fs_name() for obj in self.members.itervalues()])
        elif path_els[0] in self.members:
            dir_entries.extend(self.members[path_els[0]].fs_members(*path_els[1:]))

        logger.debug('dir entries for %s are %s' % (path, dir_entries))
        for r in dir_entries:
            yield r
            #yield fuse.Direntry(r)

    def read(self, path, size, offset, fh):
        logger.debug('read path=%s, size=%s, offset=%s fh=%s' % \
                     (path, size, offset, fh))
        path_els = path.strip('/').split('/')

        # FIXME: not very efficient
        if path_els[0] in self.members:
            str = self.members[path_els[0]].fs_read(*path_els[1:])

        if str is not None:
            slen = len(str)
            if offset < slen:
                if offset + size > slen:
                    size = slen - offset
                buf = str[offset:offset+size]
            else:
                buf = ''
            return buf

    def write(self, path, data, offset, fh):
        # TODO: fusepy adds fh, what is it used for?
        path_els = path.strip('/').split('/')
        logger.debug('write: path: %s data: %s offset: %s, fh: %s' % \
                     (path, data, offset, fh))
        # use data as the buffer to be written
        buf = data
         
        # currently only expect to be writing datastreams
        if len(path_els) == 2 and path_els[0] in self.members and \
            path_els[1] in self.members[path_els[0]].fs_members(writable_only=True):
            # ok to write for this path
            if path not in self.towrite.keys():
                if offset != 0:
                    return -errno.ENOSYS    # don't allow starting to write a file in the middle
                self.towrite[path] = MutableString()
                
            self.towrite[path][offset:len(buf)] = buf
            return len(buf)
            
        else:
            # attempting to write somtehing we don't handle
            return -errno.ENOSYS            

    def fsync(self, path, datasync, fh):
        #def fsync(self, path, isfsyncfile):
        
        # FIXME: what does isfsyncfile do ?
        # actually write the contents to fedora

        # assuming if there is data to write, file should be written (?)
        if path in self.towrite.keys():
            contents = str(self.towrite[path])
            logger.debug('fsync contents: %s' % contents)
            if contents:
                path_els = path.strip('/').split('/')
                # stuff should only get in towrite if it was determined to be writable                
                if self.members[path_els[0]].fs_write(path_els[1], contents):                
                    logger.debug('%s updated successfully' % path)

            del self.towrite[path]

        return 0


    def mknod(self, path, mode, dev):
        return 0

    def unlink(self, path):
        return 0

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

    def readlink(self, path):
        logger.debug('readlink path=%s' % path)
        # for now, the only symlink in use is /pid/relation/pid
        # FIXME: can we shift this logic to the model somehow?
        els = path.split('/')
        pid = els[-1]    #  last element is pid - link to top-level pid entry
        newpath = "../../" + pid
        return newpath

    def fuseoptref(self):
        fuse_args = fuse.FuseArgs()
        fuse.args.optlist = {"url": self.base_url,
                             "username": self.username,
                             "password": self.password,
                             "filter":  self.filter,
                             }
        return fuse_args


def main():
    usage="""
    FedoraFS: A filesystem to access content in a Fedora repository.
    """


    parser = argparse.ArgumentParser(description=usage)
    parser.add_argument('--base_url', metavar='BASE_URL',
                   help='fedora base url')
    parser.add_argument('--username', metavar='USER',
                   help='fedora username')
    parser.add_argument('--password', metavar='PASSWORD',
                   help='fedora password')
    parser.add_argument('--filter', metavar='FILTER',
                   help='keyword search filter for fedora objects to retrieve')
    parser.add_argument('mountpoint',
                   help='local mount point')
    args = parser.parse_args()

    fuse = FUSE(FedoraFS(base_url=args.base_url, username=args.username,
                         password=args.password, filter=args.filter),
                args.mountpoint, foreground=True)

    # server = FedoraFS(version="%prog " + fuse.__version__,
    #                   usage=usage, dash_s_do='setsingle')

    # # fedora-specific mount options
    # ## FIXME: could maybe use add_option_group function?
    # server.parser.add_option(mountopt="base_url", metavar="BASE_URL", default=server.base_url,
    #                          help="fedora base url [default: %default]")    
    # server.parser.add_option(mountopt="username", metavar="USER", default=server.username,
    #                          help="fedora user [default: %default]")
    # server.parser.add_option(mountopt="password", metavar="PASSWORD", default=server.password,
    #                          help="fedora password [default: %default]")
    # server.parser.add_option(mountopt="filter", metavar="FILTER", default=None,
    #                          help="keyword search filter for fedora objects to retrieve")
    # server.parse(values=server, errex=1)
    # server.main()

if __name__ == '__main__':
    main()
