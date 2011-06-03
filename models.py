# file models.py
# 
#   Copyright 2011 Emory University General Library
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

import errno   
import fuse
import stat  
from time import mktime

from eulfedora.models import DigitalObject
from eulfedora.util import parse_xml_object
from eulfedora.xml import ObjectDatastreams

class FsStat(fuse.Stat):
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


class FsObject(DigitalObject):

    method_names = {}

    def fs_name(self):
        'Return the file-system name for this object, for directory listings.'
        # MUST return a string, fuse can't handle unicode for fs names
        return str(self.pid)

    def fs_attr(self, *els):
        '''Return a file-system stat object for a requested item, so fuse can
        determine how the item should be displayed in the directory listings.       
        
        :param els: takes a list of path elements
        :rtype: :class:`FsStat`
        '''
        st = FsStat()

        # TODO: how to map fedora users to local uids? 

        if len(els) == 0:
            # no elements - the object itself
            # return as a directory
            st.st_mode = stat.S_IFDIR | 0755
            # number of items in this directory
            # currently: datastreams, methods, .info
            st.st_nlink = 2 + len(self.ds_list) + 1 # info
            st.st_nlink += len(self.related_objects.keys())
            if self.info:
                st.st_ctime = int(mktime(self.info.created.timetuple()))
                st.st_mtime = int(mktime(self.info.modified.timetuple()))

        elif els[0] == '.info':
            # .info - special case for supplying top-level object properties
            # return as a regular file
            st.st_mode = stat.S_IFREG | 0444
            st.st_size = len(self.info_text())
            st.st_nlink = 1
            st.st_ctime = int(mktime(self.info.created.timetuple()))
            st.st_mtime = int(mktime(self.info.modified.timetuple()))
            
        elif els[0] == '.versions':
            # .versions - directory of versioned view of the object
            # FIXME: versions not completely implemented

            if len(els) == 1:
                # top-level of versions: list all revisions in object history
                st.st_mode = stat.S_IFDIR | 0444
                st.st_nlink = 2 + len(self.history)
                st.st_ctime = int(mktime(self.info.created.timetuple()))    # ??
                st.st_mtime = int(mktime(self.info.modified.timetuple()))

            elif len(els) == 2:
                # single revision date-time - list datastreams (at that time)
                st.st_mode = stat.S_IFDIR | 0444
                st.st_nlink = 2 + len(self.ds_list) # FIXME: versioned!
                st.st_ctime = int(mktime(self.info.created.timetuple()))    # ??
                st.st_mtime = int(mktime(self.info.modified.timetuple()))
                
            elif len(els) == 3:
                # version of a datastream
                st.st_mode = stat.S_IFREG | 0444
                # TODO: stats for versioned datastreams
                #   -- should be able to share code for latest datastreams
                # (but make read-only)

        elif els[0] in self.method_names:
            # path element is the name of a known fedora object method/dissemination
            # return stats as if this is a regular file
            st.st_mode = stat.S_IFREG | 0444
            st.st_nlink = 1
            # using object creation/mod time for methods - what would actually make sense here?
            st.st_ctime = int(mktime(self.info.created.timetuple()))
            st.st_mtime = int(mktime(self.info.modified.timetuple()))
            try:
                # calculate the size by actually getting the content (?)
                # TODO: cache the dissemination the first time it is accessed
                diss, uri = self.getDissemination(self.method_names[els[0]], els[0])
                st.st_size = len(diss)  # FIXME: cache contents?
            except Exception:
                # if anything goes wrong, assume user does not have access
                # (could also be an error in the disseminator...)
                st.st_size = 0
                st.st_mode = stat.S_IFREG | 0000

        elif els[0] in self.ds_list:
            # path element is a datastream that belongs to this object
            # return stats as a regular file
            dsid = els[0]
            st.st_mode = stat.S_IFREG | 0644
            st.st_nlink = 1

            try:
                # attempt to get size and creation/modification times
                # from datastream profile
                profile = self.getDatastreamProfile(dsid)
                if profile:
                    st.st_size = profile.size
                    st.st_ctime = int(mktime(profile.created.timetuple()))
                    st.st_mtime = st.st_ctime
                    # FIXME: does not return mtime;  use object mtime? ctime = mtime (this *version* of DS?)
                    # ARGH: fedora apparently returns 0 size for managed datastreams ?
                    # NOTE: size for managed datastreams should be fixed in fedora 3.4

                # if that failed or we still need size (which fuse requires to display files)
                if not(profile) or st.st_size == 0:
                    # as fall-back only, get size from datastream itself
                    # Note that this very slow (& possibly inaccurate) for large datastreams
                    data, url = self.api.getDatastreamDissemination(self.pid, dsid)
                    # TODO: cache the datastream the first time it is accessed
                    # (needs a configurable timeout for reloading, detecting changes? or use ds profile?)
                    st.st_size = len(data)
            except Exception:
                st.st_size = 0
                st.st_mode = stat.S_IFREG | 0000

        elif els[0] in self.rel_shortnames:
            # relation - at top level, directory of objects
            if len(els) == 1:
                # top-level of relation: list all related objects
                st.st_mode = stat.S_IFDIR | 0444
                st.st_nlink = 2 + len(self.related_objects[self.rel_shortnames[els[0]]])
                # ?? how to determine creation/modification time for relation? rels-ext?
                st.st_ctime = int(mktime(self.info.created.timetuple()))    
                st.st_mtime = int(mktime(self.info.modified.timetuple()))

            # related object - symlink to top-level object
            if len(els) == 2:
                st.st_mode = stat.S_IFLNK | 0755

        else:
            return -errno.ENOENT    # no such file or directory

        return st

    def fs_members(self, *els, **kwargs):
        '''Get all the members of a "container" element (i.e. an object treated
        as a directory by fuse).
        
        :param els: list of path elements
        :params writable_only: if True, only return members that are capable of
            being written
        :rtype: list
        '''
        
        if 'writable_only' in kwargs:            
            writable_only = kwargs['writable_only']
        else:
            writable_only=False
            
        members = []
        if len(els) == 0:   # top level
            # no elements - top-level view of the object itself
            # find all members of this object that should be listed in the directory
            # - should be strings, not unicode
            if not writable_only:
                members.append('.info')
            if len(self.history):
                members.append('.versions')

            # add all datastreams - using datastream id for directory listing
            members.extend([str(dsid) for dsid in self.ds_list.keys()])
            # relations to other objects - use short-hand rel names for directory names
            members.extend(self.rel_shortnames)

            if not writable_only:
                # add methods - using method name for directory listing
                # NOTE: it's possible to have duplicate method names defined
                # on different service objects... current implementation does
                # *not* take this into account
                self.method_names = {}
                for sdef, method_list in self.methods.iteritems():
                    # FIXME: any way to exclude methods that require parameters?
                    for m in method_list:
                        self.method_names[m] = sdef

                members.extend(self.method_names)
            
        elif els[0] == '.versions':
            # list revisions of this object
            # (not completely implemented...)
            if len(els) == 1:
                members = [str(dt) for dt in self.history]
            else:
                # TODO: get a *versioned* list of datastreams
                # (don't list ones that didn't exist at the specified time!)
                data, url = self.api.listDatastreams(self.pid)
                dsobj = parse_xml_object(ObjectDatastreams, data, url)
                return [ str(ds.dsid) for ds in dsobj.datastreams ]

        # related objects
        elif els[0] in self.rel_shortnames:
            # top level: directory with objects related by the specified short-name relation
            if len(els) == 1:
                rel_uri = self.rel_shortnames[els[0]]
                members = self.related_objects[rel_uri]

        return members

    def fs_read(self, *els):
        '''Return the content of the requested element
        :param els: list of path elements
        '''
        if els[0] == '.info':
            # top-level object info
            return self.info_text()
        elif els[0] in self.method_names:
            # dissemination content
            diss, uri = self.getDissemination(self.method_names[els[0]], els[0])
            return diss
        elif els[0] in self.ds_list:
            # datastream content
            # FIXME: somewhere a newline is getting prepended to datastream content (?)
            # TODO: update api to return a file-like object so it can be read in chunks
            data, url = self.api.getDatastreamDissemination(self.pid, els[0])
            return data
        
        elif els[0] in '.versions':
            # only thing currently readable under .versions - versioned datastreams
            if len(els) == 3:
                # 3 entries under .versions - versioned view of a datastream
                date, dsid = els[1:]
                # FIXME: must be a better way to get datetime back from string...
                for dt in self.history:
                    if date == str(dt):
                        datetime = dt

                # TODO: add caching? versioned ds content shouldn't change
                data, url = self.api.getDatastreamDissemination(self.pid, dsid, datetime)
                return data


    def fs_write(self, dsid, data):
        'Write content to a datastream'
        # only makes sense to write a single datastream
        # how to define datastream objects dynamically ?
        dsobj = self.getDatastreamObject(dsid)
        dsobj.content = data
        return dsobj.save()

    def info_text(self):    # versioned?
        'Generate .info file contents based on the object profile'
        lines = ["object info for " + self.pid + "\n\n"]
        for field in ['label', 'owner', 'created', 'modified', 'state']:
            lines.append('%s:\t%s\n' % (field, getattr(self.info, field)))
        return ''.join(lines).encode('ascii')


    
    _rel_shortnames = None
    @property
    def rel_shortnames(self):
        if self._rel_shortnames is None:
            # initialize related objects & equivalent short names
            self.related_objects
        return self._rel_shortnames


    _rel_objects = None
    @property
    def related_objects(self):
        if self._rel_objects is None:
            if self._rel_shortnames is None:
                self._rel_shortnames = {}
            self._rel_objects = {}
            for subj, pred, obj in self.rels_ext.content:
                if str(obj).startswith('info:fedora/'):
                    rel = str(pred)
                    if rel not in self._rel_objects:
                        self._rel_objects[rel] = []
                    # use short-hand pid instead of uri as link name
                    self._rel_objects[rel].append(str(obj).replace('info:fedora/', ''))
                    # generate a short-hand name to be used as directory
                    # - for now, use the local relation predicate without namespace as directory name
                    self._rel_shortnames[rel[rel.find('#')+1:]] = rel
        return self._rel_objects
            
            
