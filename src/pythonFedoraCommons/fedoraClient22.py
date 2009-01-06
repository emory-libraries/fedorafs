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

import codecs, os

# For the SOAP actions
import ZSI
# Automatically generated from the WSDL for API_A and API_M
from Fedora_API_M_WSDL_services import *
from Fedora_API_A_WSDL_services import *

from mimeTypes import *
from upload import *

from xml.dom import minidom
from xml.dom import Node

# Metadata handlers
from foxml import *

class FedoraClient(object):
    def __init__(self, serverurl='http://localhost:8080/fedora', username='fedoraAdmin', password='fedoraAdmin', version="2.2"):
        self.server = serverurl
        
        #Choose which method of testing for object existance
        self.doesObjectExist = self.doesObjectExist_REST
        self.listDatastreams = self.listDatastreams_SOAP
        self.doesDatastreamExist = self.doesDatastreamExist_SOAP
        
        m = mimeTypes()
        self.mimetypes = m.getDictionary()
        
        self.version = "2.2"
        
        access_tail = '/services/access'
        management_tail = '/services/management'

        # Setup API-A SOAP proxy
        api_a_kw={'auth' : (ZSI.auth.AUTH.httpbasic, username, password), 'url' : self.server + access_tail}
        api_a_loc = Fedora_API_A_ServiceLocator()
        self.api_a = api_a_loc.getFedora_API_A(**api_a_kw)

        # Setup API-M SOAP proxy
        api_m_kw={'auth' : (ZSI.auth.AUTH.httpbasic, username, password), 'url' : self.server + management_tail}
        api_m_loc = Fedora_API_M_ServiceLocator()
        self.api_m = api_m_loc.getFedora_API_M(**api_m_kw)
        
        self.uploader = FedoraUpload(self.server)

    def _getText(self, nodelist):
        text = ''
        for node in nodelist:
            if node.nodeType == Node.TEXT_NODE:
                text += node.nodeValue
        return text
        
    def listDatastreams_REST(self, pid):
        # From http://ora.ouls.ox.ac.uk:8080/fedora/listDatastreams/namespace:id?xml=true
        xmllist = self.getListDatastreams(pid)
        if not isinstance(xmllist, int):
            # try to parse xml, and respond with a dict of dsids and attributes
            dom = minidom.parseString(xmllist)
            # get the datastream nodes out
            datastreamNodes = [x for x in dom.documentElement.childNodes if x.nodeName == 'datastream']
            dslist = {}
            for node in datastreamNodes:
                # read the attribs and make a dict from them
                attribs = {}
                for index in xrange(0,  len(node.attributes)):
                    attrib_ref = node.attributes.item(index)
                    attribs[attrib_ref.name] = attrib_ref.value
                    
                if attribs['dsid']:
                    dslist[attribs['dsid']] = attribs
            return dslist
        else:
            return xmllist
    
    
    def getListDatastreams(self, pid):
        url = self.server + '/listDatastreams/' + pid + '?xml=true'
        try:
            response = urllib2.urlopen( url )
            return response.read()
        except urllib2.HTTPError, exc:
            return exc.code

    # REST Url for a GET
    def getDatastreamUrl(self, pid, ds):
        return self.server + '/get/' + pid + '/'+ ds

    # REST Url for a GET
    def getProfileUrl(self, pid):
        return self.server + '/get/' + pid

    # REST GET
    def getDatastream(self, pid, ds):
        url = self.getDatastreamUrl(pid, ds)
        try:
            response = urllib2.urlopen( url )
            return response.read()
        except urllib2.HTTPError, exc:
            return exc.code

    # REST GET (with ?xml=true)
    def getObjectProfile(self, pid, format="xml"):
        url = self.getProfileUrl(pid)
        queryparams = urllib.urlencode({'xml' : 'true'})
        try:
            response = urllib2.urlopen( url, queryparams )
            xml_resp = response.read()
            if xml_resp:
                if format=="xml":
                    return xml_resp
                else:
                    # try to parse xml, and respond with a dict of dsids and attributes
                    dom = minidom.parseString(xml_resp)
                    # get the datastream nodes out
                    object_profile_list = {}
                    for node in dom.documentElement.childNodes:
                        object_profile_list[node.nodeName] = self._getText(node.childNodes)
                    return object_profile_list
        except urllib2.HTTPError, exc:
            return exc.code

    # REST GET
    def getOwnerID(self, pid):
        if self.doesObjectExist(pid):
            profile = self.getObjectProfile(pid)
            if not isinstance(profile, int):
                doc = minidom.parseString(profile).documentElement
                contentModel = self._getText(doc.getElementsByTagName('objOwnerId')[0].childNodes)
                return contentModel
        return None

    def getContentModel(self, pid):
        if self.doesObjectExist(pid):
            profile = self.getObjectProfile(pid)
            if not isinstance(profile, int):
                doc = minidom.parseString(profile).documentElement
                contentModel = self._getText(doc.getElementsByTagName('objContentModel')[0].childNodes)
                return contentModel
        return None

    def getDescriptionXML(self, format="xml"):
        url = self.server + '/describe'
        queryparams = urllib.urlencode({'xml' : 'true'})
        try:
            response = urllib2.urlopen( url, queryparams )
            xml_resp = response.read()
            if xml_resp:
                if format=="xml":
                    return xml_resp
                else:
                    # try to parse xml, and respond with a dict of dsids and attributes
                    dom = minidom.parseString(xml_resp)
                    # get the datastream nodes out
                    repo_profile_list = {}
                    for node in dom.documentElement.childNodes:
                        if node.nodeName == 'repositoryPID':
                            # Get the list of retained pid namespaces from this element set
                            retainpid_nodelist = [x for x in node.childNodes if x.nodeName=='retainPID']
                            retainpid_list = []
                            for retainpid_node in retainpid_nodelist:
                                retainpid_list.append(self._getText(retainpid_node.childNodes))
                            
                            repo_profile_list['retainPID'] = retainpid_list
                        else:
                            repo_profile_list[node.nodeName] = self._getText(node.childNodes)
                    return repo_profile_list
        except urllib2.HTTPError, exc:
            return exc.code

    def getDescription(self):
        url = self.server + '/describe'
        try:
            response = urllib2.urlopen( url )
            return response.read()
        except urllib2.HTTPError, exc:
            return exc.code
           
    def getObjectProfile_SOAP(self, pid):
        request = getObjectProfileRequest()
        
        request._pid = pid
        
        return self.api_a.getObjectProfile(request)
        
    def getDatastreamProfile(self, id, dsid, asOfDateTime=None):
        
        request = getDatastreamRequest()
        request._pid = id
        request._dsID = dsid
        if asOfDateTime:
            request._asOfDateTime = asOfDateTime
            
        response = self.api_m.getDatastream(request)
        
        return response
        
    def modifyDSByValue(self, id, dsid, altids, dsLabel, mimeType, formatURI, dsContent, checksumType, checksum, logMessage, force):

        request = modifyDatastreamByValueRequest()
        request._pid = id
        request._MIMEType = mimeType
        request._altIDs = altids
        request._checksum = checksum
        request._checksumType = checksumType
        request._dsContent = dsContent.encode("UTF-8")
        request._dsID = dsid
        request._dsLabel = dsLabel
        request._force = force
        request._formatURI = formatURI
        request._logMessage = logMessage
 
        response = self.api_m.modifyDatastreamByValue(request)
        
        return response
        
    def addDatastream(self, id, dsid, dsLabel, versionable, mimeType, formatURI, dsLocation, controlGroup, dsState, checksumType, checksum, logMessage):
    
        request = addDatastreamRequest()
        request._pid = id
        request._MIMEType = mimeType
        request._altIDs = ns0.ArrayOfString_Def('').pyclass
        request._checksum = checksum
        request._checksumType = checksumType
        request._dsLocation = dsLocation
        request._dsID = dsid
        request._dsLabel = dsLabel
        request._formatURI = formatURI
        request._logMessage = logMessage
        request._versionable = versionable
        request._controlGroup = controlGroup
        request._dsState = dsState
        
        response = self.api_m.addDatastream(request)

        return response
        
    def addString(self, id, dsid, dsLabel, versionable, XMLstring, controlGroup, dsState, logMessage, permanent_store, mimetype="text/xml"):
        ext = u'.xml'
        if mimetype != "text/xml":
            # If not XML, then fall back to text/plain and .txt
            ext = u'.txt'
        filename = os.path.join(permanent_store,id+dsid+ext)
        permanent_file = None
        if mimetype == 'text/xml':
            # Convention: All XML is stored as UTF-8
            permanent_file = codecs.open(filename, mode='w', encoding='utf-8')
        else:
            permanent_file = open(filename, 'w')
        if permanent_file:
            permanent_file.write(XMLstring)
            permanent_file.close()
            return self.addDS(id, dsid, filename, dsLabel, logMessage, controlGroup, versionable, dsState)
        else:
            return None
        
    def modifyDSByReference(self, id, dsid, altids, dsLabel, mimeType, formatURI, dsLocation, checksumType, checksum, logMessage, force):

        request = modifyDatastreamByReferenceRequest()
        request._pid = id
        request._MIMEType = mimeType
        request._altIDs = altids
        request._checksum = checksum
        request._checksumType = checksumType
        request._dsLocation = dsLocation
        request._dsID = dsid
        request._dsLabel = dsLabel
        request._force = force
        request._formatURI = formatURI
        request._logMessage = logMessage
 
        response = self.api_m.modifyDatastreamByReference(request)

        return response

    def getNextPID(self, namespace=u"ora"):
        request = getNextPIDRequest()
        request._pidNamespace = namespace

        response = self.api_m.getNextPID(request)
        return response

    def ingest(self, objectXML, logMessage='Ingest object', format='foxml1.0'):
        request = ingestRequest()
        request._objectXML = objectXML
        request._logMessage = logMessage
        request._format = format

        response = self.api_m.ingest(request)
        return response
        
    def purgeDatastream(self, pid, dsid, logMessage='deleted datastream'):
        request = purgeDatastreamRequest()
        request._pid = pid
        request._logMessage = logMessage
        request._dsID = dsid
        request._force = False

        response = self.api_m.purgeDatastream(request)
        return response
        
    def purgeObject(self, pid, logMessage='deleted object'):
        request = purgeObjectRequest()
        request._pid = pid
        request._logMessage = logMessage
        request._force = False

        response = self.api_m.purgeObject(request)
        return response

    def modifyObject(self, pid, label, ownerId, logMessage='Modified object', state='A' ):
        request = modifyObjectRequest()
        request._pid = pid
        request._state = state
        request._label = label
        request._logMessage = logMessage
        request._ownerId = ownerId

        response = self.api_m.modifyObject(request)

        return response

    def replace(self, id, dsid, filename, label, logMessage):
        # Do file upload:
        loc = self.uploader.putdata(filename)
        mimeType = self.uploader.get_content_type(filename)

        dsLocation = loc
        altids = ns0.ArrayOfString_Def('').pyclass
        dsLabel = label
        logMessage = logMessage
        response = self.modifyDSByReference(id, dsid, altids, dsLabel, mimeType, '', dsLocation, 'DISABLED', 'none', logMessage, False)
        
        return response
        
    def getBlankAltIDs(self):
        return ns0.ArrayOfString_Def('').pyclass    
        
    def addDS(self, id, dsid, filename, label, logMessage, controlGroup, versionable, state):
        # Do file upload:
        loc = self.uploader.putdata(filename)
        mimeType = self.uploader.get_content_type(filename)

        """def addDatastream(self, id, dsid, altids, dsLabel, 
                                   versionable, mimeType, formatURI, 
                                   dsLocation, controlGroup, dsState, 
                                   checksumType, checksum, logMessage):"""
        dsLocation = loc
        altids = ns0.ArrayOfString_Def('').pyclass
        dsLabel = label
        logMessage = logMessage
        # Try to remove temporary file
        try:
            os.remove(filename)
        except OSError:
            # Bugger, either the file doesn't exist, or the permissions are wrong
            pass
        
        return self.addDatastream(id, dsid, dsLabel, versionable, mimeType, '', dsLocation, controlGroup, state, 'DISABLED', 'none', logMessage)
        
    def doesObjectExist_SOAP(self, pid):
        # Run a query to find the datastreams for this PID
        # Catches the exception thrown if the PID cannot be
        # accessed or doesn't exist
        if pid == None:
            return False

        request = getDatastreamsRequest()
        request._pid =  pid

        try:
            response = self.api_m.getDatastreams(request) 
        except ZSI.FaultException:
            return False
        
        return True
        
    def doesObjectExist_REST(self, pid):
        # REST style query
        response = self.getObjectProfile(pid)
        
        if response != '':
            return True
        
        return False
        
        
    def doesDatastreamExist_SOAP(self, pid, dsid):
        # Run a query to find the datastreams for this PID
        # Check for existance of a given pid, return True or
        # False depending.
        
        if dsid == None or pid == None:
            return False

        request = getDatastreamsRequest()
        request._pid =  pid

        try:
            response = self.api_m.getDatastreams(request) 
        except ZSI.FaultException:
            return False       

        dslist = response._datastream

        for ds in dslist:
            if ds._ID == dsid:
                return True

        return False
    def doesDatastreamExist_REST(self, pid, dsid):
        # Let the fall through error on access dictate:
        url = self.getDatastreamUrl(pid, dsid)
        try:
            response = urllib2.urlopen( url )
            response.close()
            return True
        except urllib2.HTTPError, exc:
            return False
        

    def listDatastreams_SOAP(self, pid):
        # Run a query to find the datastreams for this PID
        # Changed from API-A-Lite access to SOAP API-M due to
        # the lack of information in the former.

        request = getDatastreamsRequest()
        request._pid =  pid

        response = self.api_m.getDatastreams(request)        

        dslist = response._datastream
        
        # Split the pid, to include in a windows-friendly filename (i.e. no ':')
        namespace, id_number = pid.split(':')

        datastreams = {}
#        datastreams = []
        for ds in dslist:
            datastream = {}
            datastream['pid'] = pid
            datastream['dsid'] = ds._ID
            datastream['mimetype'] = ds._MIMEType
            # TODO: Parse list -> _altIDs and spit out CSV or whatever. Field has no use to me atm though.
            datastream['checksum'] = ds._checksum
            datastream['checksumtype'] = ds._checksumType
            datastream['controlgroup'] = ds._controlGroup
            datastream['createdate'] = ds._createDate
            datastream['formaturi'] = ds._formatURI
            datastream['label'] = ds._label
            datastream['location'] = ds._location
            datastream['size'] = ds._size
            datastream['state'] = ds._state
            datastream['versionid'] = ds._versionID
            datastream['versionable'] = ds._versionable

            # Choose a known file extension from the dictinary list or
            # default to the 'possible_ending' variable. This is added to the
            # dsid to form a download filename
            # e.g.    dsid = MODS, mimetype = 'text/xml'
            # mimetypes['text/xml'] = 'xml' so the filename is 'MODS.xml'
            # 
            # if dsid = TDNET and mimetype = application/mrsid
            # as mimetypes['application/mrsid'] is undefined, the filename is
            # 'TDNET.mrsid'
            possible_ending = datastream['mimetype'].split('/')[1]
            datastream['winname'] = namespace+'_'+id_number+'-'+datastream['dsid'] + '.' + self.mimetypes.get(datastream['mimetype'], possible_ending)
            if datastream['dsid']:
                    datastreams[datastream['dsid']] = datastream
#            datastreams.append(datastream)

        return datastreams
