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

import httplib
import urlparse
import urllib
import base64
from base64 import encodestring

from mimeTypes import *

import mimetypes

from cStringIO import StringIO

class Connection:
    def __init__(self, base_url, username, password):
        self.base_url = base_url
        self.username = username
        self.password = password
        m = mimeTypes()
        self.mimetypes = m.getDictionary()
        
        self.url = urlparse.urlparse(base_url)
        self.host, self.query = self.url[1], self.url[2]
        
    def authtobasic(self, username, password): 
        """Converts basic auth data into an HTTP header."""
        userpass = username+":"+password
        userpass = encodestring(urllib.unquote(userpass)).strip()
        return 'Basic '+userpass
        
    def request_get(self, resource, args = None):
        return self.request(resource, "get", args)
        
    def request_delete(self, resource, args = None):
        return self.request(resource, "delete", args)
        
    def request_post(self, resource, args = None, body = None, filename=None):
        return self.request(resource, "post", args , body = body, filename=filename)
        
    def get_content_type(self, filename):
        extension = filename.split('.')[-1]
        guessed_mimetype = self.mimetypes.get(extension, mimetypes.guess_type(filename)[0])
        return guessed_mimetype or 'application/octet-stream'
        
    def request(self, resource, method = "get", args = None, body = None, filename=None):
        params = None
        path = resource
        headers = {'User-Agent': 'Basic Agent'}
        
        BOUNDARY = u'00hoYUXOnLD5RQ8SKGYVgLLt64jejnMwtO7q8XE1'
        CRLF = u'\r\n'
        
        if filename and body:
            #fn = open(filename ,'r')
            #chunks = fn.read()
            #fn.close()
            
            # Attempt to find the Mimetype
            content_type = self.get_content_type(filename)
            headers['Content-Type']='multipart/form-data; boundary='+BOUNDARY
            encode_string = StringIO()
            encode_string.write(CRLF)
            encode_string.write(u'--' + BOUNDARY + CRLF)
            encode_string.write(u'Content-Disposition: form-data; name="file"; filename="%s"' % filename)
            encode_string.write(CRLF)
            encode_string.write(u'Content-Type: %s' % content_type + CRLF)
            encode_string.write(CRLF)
            encode_string.write(body)
            encode_string.write(CRLF)
            encode_string.write(u'--' + BOUNDARY + u'--' + CRLF)
            
            body = encode_string.getvalue()
            headers['Content-Length'] = str(len(body))
        elif body:
            headers['Content-Type']='text/xml'
            headers['Content-Length'] = str(len(body))
            
        else: 
            headers['Content-Type']='text/xml'
            
        if args:
            path += "?" + urllib.urlencode(args)
        if self.username and self.password:
            headers["Authorization"] = self.authtobasic(self.username, self.password)
        if (self.url.port == 443):
            conn = httplib.HTTPSConnection(self.host)
        else:
            conn = httplib.HTTPConnection(self.host)
        
        conn.request(method.upper(), self.query+"/" + path, body, headers)
        
        return conn.getresponse().read().strip()
