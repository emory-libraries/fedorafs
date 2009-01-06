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

import sys, string, re, os, md5, sha, time
import httplib, urlparse, urllib, urllib2, mimetypes

from mimeTypes import *

from base64 import encodestring

from cStringIO import StringIO

# Basic Authentication
class FedoraUpload(object):
    def __init__(self, fedora_url, type='Basic', username='fedoraAdmin', password='fedoraAdmin'):
        self.uploadurl = fedora_url + '/management/upload'
        self.error = None
        self.h = None
        self.set_auth(type, username, password)
        
        m = mimeTypes()
        self.mimetypes = m.getDictionary()
        
        # Make the request object from the supplied uri
        u = urlparse.urlparse(self.uploadurl)
        self.host, self.query = u[1], u[2]
        
        self.make_connection()

    def set_auth(self, type, username, password):
        self.auth = {'type':type, 
                     'username':username,
                     'password':password}

    def make_connection(self):
        if self.h != None:
            self.h.close()
        self.h = httplib.HTTPConnection(self.host)

    def authtobasic(self): 
        """Converts basic auth data into an HTTP header."""
        userpass = self.auth['username']+':'+self.auth['password']
        userpass = encodestring(urllib.unquote(userpass)).strip()
        return 'Basic '+userpass

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # 
    # D I G E S T    A U T H E N T I C A T I O N    S T U F F
    # These functions are based on the stuff in urllib2
    # 

    def authtodigest(self, method='PUT'):
        user, pw, a = self.auth['username'], self.auth['password'], self.auth['data']
        x = self.http_digest_auth(a, user, pw, method)
        self.h.putheader('Authorization', x)

    def http_digest_auth(self, a, user, pw, method):
        token, challenge = a.split(' ', 1)
        chal = urllib2.parse_keqv_list(urllib2.parse_http_list(challenge))
        a = self.get_authorization(chal, user, pw, method)
        if a: return 'Digest %s' % a

    def get_authorization(self, chal, user, pw, method):
        try:
            realm, nonce = chal['realm'], chal['nonce']
            algorithm, opaque = chal.get('algorithm', 'MD5'), chal.get('opaque', None)
        except KeyError: return None
        H, KD = self.get_algorithm_impls(algorithm)
        if H is None: return None
        A1, A2 = "%s:%s:%s" % (user, realm, pw), "%s:%s" % (method, self.uploadurl)
        respdig = KD(H(A1), "%s:%s" % (nonce, H(A2)))
        base = 'username="%s", realm="%s", nonce="%s", uri="%s", ' \
                     'response="%s"' % (user, realm, nonce, self.uploadurl, respdig)
        if opaque: base = base + ', opaque="%s"' % opaque
        if algorithm != 'MD5': base = base + ', algorithm="%s"' % algorithm
        return base

    def get_algorithm_impls(self, algorithm):
        if algorithm == 'MD5':
            H = lambda x, e=urllib2.encode_digest:e(md5.new(x).digest())
        elif algorithm == 'SHA':
            H = lambda x, e=urllib2.encode_digest:e(sha.new(x).digest())
        KD = lambda s, d, H=H: H("%s:%s" % (s, d))
        return H, KD

    # 
    # End of Digest Authentication functions
    # 
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

    def putdata(self,filename):
        fn = open(filename ,'r')
        chunks = fn.read()
        fn.close()
        
        # Attempt to find the Mimetype
        content_type = self.get_content_type(filename)
        
        # Boundary text:
        BOUNDARY = u'00hoYUXOnLD5RQ8SKGYVgLLt64jejnMwtO7q8XE1'
        CRLF = u'\r\n'
        
        encode_string = StringIO()
        encode_string.write(CRLF)
        encode_string.write(u'--' + BOUNDARY + CRLF)
        encode_string.write(u'Content-Disposition: form-data; name="file"; filename="%s"' % filename)
        encode_string.write(CRLF)
        encode_string.write(u'Content-Type: %s' % content_type + CRLF)
        encode_string.write(CRLF)
        encode_string.write(chunks)
        encode_string.write(CRLF)
        encode_string.write(u'--' + BOUNDARY + u'--' + CRLF)
        
        body = encode_string.getvalue()
        
        headers = {
        'User-Agent': 'RDFBrowse-Uploader/1.0',
        'Content-Type': 'multipart/form-data; boundary='+BOUNDARY,
        'Content-Length': str(len(body))
        }
        
        if 'type' in self.auth.keys(): 
            if self.auth['type'] == 'Basic':
                headers['Authorization'] = self.authtobasic()        

        self.h.request('POST', self.query, body, headers)
        res = self.h.getresponse()
        return res.read().strip()
        
#        return res.status, res.reason, res.read().strip()
        
        
    def get_content_type(self, filename):
        extension = filename.split('.')[-1]
        guessed_mimetype = self.mimetypes.get(extension, mimetypes.guess_type(filename)[0])
        return guessed_mimetype or 'application/octet-stream'
"""
        if errcode in (301, 302): 
            self.error = 'PUT data error code was '+str(errcode)
        elif errcode == 401: 
            self.error = 'Authorization failed!\n'+'Auth: '+headers['www-authenticate']
        else: 
            "" "  'Done: '+str(errcode)+': '+str(errmsg)+'\n'+str(headers)) " ""
            if errcode in (200, 201, 204): 
                sys.stderr.write('PUT succeeded!')
                # perr(body)
            elif errcode == 405: sys.stderr.write('PUT failed!')
            elif errcode == 404: perr('PUT failed: 404!')"""
