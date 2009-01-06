"""
    Copyright (C) 2008 Benjamin O'Steen

    This file is part of python-fedoracommons.

    python-fedoracommons is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    python-fedoracommons is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with python-fedoracommons.  If not, see <http://www.gnu.org/licenses/>.
"""

__license__ = 'GPL http://www.gnu.org/licenses/gpl.txt'
__author__ = "Benjamin O'Steen <bosteen@gmail.com>"
__version__ = '0.1'


import urllib2
import urllib
import httplib

from restful_lib import Connection
from risearch import Risearch

import re

from base64 import encodestring

from uuid import uuid4

import codecs, os

from mimeTypes import *

import mimetypes

from xml.dom import minidom
from xml.dom import Node

from elementtree import ElementTree as ET

class FedoraClient(object):
    def __init__(self, server='http://localhost:8080/fedora', username='fedoraAdmin', password='fedoraAdmin', version="3.0", use_UUID=False):
        self.server = server
        self.username = username
        
        self.use_UUID = use_UUID
        
        self.ri = Risearch(server=server)
        
        if not (version == "3.0"):
            raise "This library only provides an interface to a Fedora 3.0 repository with REST API enabled"
        
        if use_UUID:
            self.createNewObject = self.create_UUID_Object
        else:
            self.createNewObject = self._createNewObject
        
        self.uuid_id_regex = re.compile('[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}')
        
        m = mimeTypes()
        self.mimetypes = m.getDictionary()
        self.fedora = Connection(self.server, self.username, password)
        
        self.version = version

    def _getText(self, nodelist):
        text = ''
        for node in nodelist:
            if node.nodeType == Node.TEXT_NODE:
                text += node.nodeValue
        return text

    def getNextPID(self, namespace='ora'):
        xml_pidrequest = self.fedora.request_get('objects/nextPID', args = {'namespace':namespace, 'format':'xml'})
        
        # TODO: Error handling here
        
        piddom = minidom.parseString(xml_pidrequest)
        
        # Needs Error handling
        pid = self._getText(piddom.documentElement.childNodes[1].childNodes)
        
        return pid
        
    def getSingleUUID(self):
        u = uuid4()        
        return u.urn[4:]

    def _createNewObject(self, label=None, pid=None):
        if not pid:
            pid = self.getNextPID()
        
        if not label:
            label='Blank Object'

        params = {'label':label, 
                'namespace':pid}
        
        resp = self.fedora.request_post("objects/"+pid, args=params)
        
        return pid

    def create_UUID_Object(self, label=None, pid=None, namespace='demo', params={}):
        """ This method is designed to be called with either no parameters or just a label parameter.
            Doing so will automatically create a new object, with a UUID pid. By calling the method with
            the pid keywofrom elementtree import ElementTree as ETrd set, will force an attempt to create an object with the given pid instead."""
        if not label:
            label = self.getNextPID(namespace=namespace)

        if not pid:
            u = uuid4()
            pid = u.urn[4:]
            params['labelfrom elementtree import ElementTree as ET'] = label
            params['namespace'] = 'uuid'
        
        resp = self.fedora.request_post("objects/"+pid, args=params)
        
        return {'pid':pid, 'tinypid':label}
        

    def resolve_Non_UUID_pid(self, pid):
        ns, id = pid.split(':')
        
        if not (ns == "uuid" and self.uuid_id_regex.match(id)):
            # lookup real pid:
            guessed_uuid = self.ri.resolveTinyPid(pid)
            
            if guessed_uuid:
                pid = guessed_uuid
            else:
                return False
        return pid
        
    def get_tiny_pid(self, uuid_pid):
        ns, id = uuid_pid.split(':')
        
        if (ns == "uuid" and self.uuid_id_regex.match(id)):
            # lookup real pid:
            guessed_tinypid = self.ri.simplifyUUID(uuid_pid)
            
            if guessed_tinypid:
                uuid_pid = guessed_tinypid
            else:
                return False
        return uuid_pid

    def getDatastream(self, pid, dsid, params=None):
        # e.g http://localhost:8080/fedora/objects/test:02/datastreams/DC
        
        # Tinypid? or uuid?
        
        if self.use_UUID:
            pid = self.resolve_Non_UUID_pid(pid) or pid
        
        return self.fedora.request_get("objects/"+pid+"/datastreams/"+dsid, args=params)
        
    def getDatastreamUrl(self, pid, dsid):
        if self.use_UUID:
            pid = self.resolve_Non_UUID_pid(pid) or pid
        return self.server + "/objects/"+pid+"/datastreams/"+dsid
        
    def putString(self, pid, dsid, content, fake_filename=None, params=None):
        if self.use_UUID:
            pid = self.resolve_Non_UUID_pid(pid) or pid
        
        return self.fedora.request_post("objects/"+pid+"/datastreams/"+dsid, args=params, body=content, filename=fake_filename)
        
    def putFile(self, pid, dsid, filepath, params=None):
        fn = open(filepath ,'r')
        content = fn.read()
        fn.close()
        
        filename = filepath.split('/')[-1]
        
        if self.use_UUID:
            pid = self.resolve_Non_UUID_pid(pid) or pid
        
        return self.fedora.request_post("objects/"+pid+"/datastreams/"+dsid, args=params, body=content, filename=filename)
    
    def listDatastreams(self, pid, format='xml', params=None):
        if self.use_UUID:
            pid = self.resolve_Non_UUID_pid(pid) or pid
        
        namespace, id_number = pid.split(':')
        
        arg_list = {'format':'xml'}
        if params and isinstance(params, dict):
            arg_list.update(params)
        if format=='python':
            xml_list = self.fedora.request_get("objects/"+pid+"/datastreams", args=arg_list)
            tree_list = ET.fromstring(xml_list)
            dsid_list = tree_list.findall('datastream')
            dsids = {}
            for dsid in dsid_list:
                datastream = {}
                datastream['dsid'] = dsid.get('dsid')
                datastream['label'] = dsid.get('label')
                datastream['mimetype'] = dsid.get('mimeType')

                # Choose a known file extension from the dictinary list or
                # default to the 'possible_ending' variable. This is added to the
                # dsid to form a download filename
                # e.g.    dsid = MODS, mimetype = 'text/xml'
                # mimetypes['text/xml'] = 'xml' so the filename is 'MODS.xml'
                ext = self.mimetypes.get(datastream['mimetype'], None)
                if not ext:
                    datastream['winname'] = 'uuid_'+id_number+'-'+datastream['dsid'] + mimetypes.guess_extension(datastream['mimetype'])
                else:
                    datastream['winname'] = 'uuid_'+id_number+'-'+datastream['dsid'] + '.' + ext
                
                if datastream['dsid']:
                    dsids[datastream['dsid']] = datastream
            
            return dsids
        else:
            return self.fedora.request_get("objects/"+pid+"/datastreams", args=arg_list)
    
    def retrieveObjectXML(self, pid):
        if self.use_UUID:
            pid = self.resolve_Non_UUID_pid(pid) or pid
        
        return self.fedora.request_get("objects/"+pid+"/objectXML", args={'format':'xml'})
        
    def getObjectProfile(self, pid, format='xml'):
        if self.use_UUID:
            pid = self.resolve_Non_UUID_pid(pid) or pid
            
        if format=='python':
            xml_list = self.fedora.request_get("objects/"+pid, args={'format':'xml'})
            tree_list = ET.fromstring(xml_list)
            profile = {}
            for node in tree_list:
                profile[node.tag] = node.text
            return profile
        else:
            return self.fedora.request_get("objects/"+pid, args={'format':'xml'})
        
    def getObjectHistory(self, pid, date=None):
        if self.use_UUID:
            pid = self.resolve_Non_UUID_pid(pid) or pid
           
        version = '/versions'
         
        if date:
            version = '/'+date
        
        return self.fedora.request_get("objects/"+pid+version, args={'format':'xml'})
        
        
    def exportObject(self, pid):
        if self.use_UUID:
            pid = self.resolve_Non_UUID_pid(pid) or pid
        
        return self.fedora.request_get("objects/"+pid+"/export", args={'format':'foxml1.0', 'context':'private'})
     
    def deleteObject(self, pid):
        if pid:
            if self.use_UUID:
                pid = self.resolve_Non_UUID_pid(pid) or pid
            return self.fedora.request_delete("objects/"+pid)
        else:
            return False

    def deleteDatastream(self, pid, dsid):
        if pid and dsid:
            if self.use_UUID:
                pid = self.resolve_Non_UUID_pid(pid) or pid
            return self.fedora.request_delete("objects/"+pid+"/datastreams/"+dsid)
        else:
            return False
