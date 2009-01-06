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

from xml.dom import minidom
from xml.dom import Node

import rdflib
from rdflib.Graph import ConjunctiveGraph as cg
from rdflib import Namespace, Literal
from rdflib import URIRef

# To directly decode the N-Triples from Trippi
from rdflib.syntax.parsers.ntriples import NTriplesParser

from datetime import datetime

import urllib
import string
import re, csv

class Risearch(object):
    def __init__(self, server='http://localhost:8080/fedora'):
        self.baseurl = server
        self.server = server + '/risearch'
        self.fedoraClient = None
        
        # HACK:  (Not needed now - 10Aug2007 - due to trippi changes)
        # Calling findTriples with a full triple (i.e. no wildcards) returns nothing
        # if the triple doesn't exist, or returns the same triple if it does exist.
        
        # This will match a single triple response in N-Triple format
        # self.triple_pattern = re.compile('^<.*?> <.*?> <.*?> .\n')
        
    def getSearchResultSet(self, query, desiredformat='Sparql', limit='10'):
        # Get Tuples from the risearch servlet running on the Fedora
        # server
        response = self.getTuples(query, format=desiredformat, limit=limit)
        
        # Instantiate items hash
        items = {}
        
        # Create a minidom reader
        if desiredformat == 'Sparql':
            # Create minidom. SAX parsing may be a good match here, but I
            # feel in a DOM kinda mood.
            doc = minidom.parseString(response).documentElement
            
            
            # Select all the <result> elements and subelements
            results = doc.getElementsByTagName('result')
            
            for resultset in results:
                # Select the child elements of the <result> element,
                # as the <result> element is there only to group them
                results = resultset.childNodes
                
                # Select out the first <object> element
                object = resultset.getElementsByTagName('object')[0]
                
                # Select out the first <dctitle> element
                dctitle = resultset.getElementsByTagName('dctitle')[0]

                # <object> is normally formatted in the following way
                # in Sparql:  <object uri="info:fedora/namespace:id"/>
                # Manipulate node to get the 'namespace:id' text, the
                # pid of the object in question
                fedora_uri = object.getAttribute('uri')
                parsed_fedora_uri = fedora_uri.split('/')

                # Check to see if it is a pid URI. Fail if not.
                if parsed_fedora_uri[0] == 'info:fedora':
                    pid = parsed_fedora_uri[1]
                else:
                    # Pass None to show failure
                    pid = None
                    
                items[pid] = self._getText(dctitle.childNodes)
        
        elif desiredformat == 'csv':
            # Much quicker to parse
            csv_reader = csv.reader(response.split("\n"))
            headers = None
            for row in csv_reader:
                if headers == None:
                    headers = row
                else:
                    # By convention, anticipate that the first item in the row is a pid:
                    if len(row)>0:
                        parsed_fedora_uri = row[0].split('/')
                        if parsed_fedora_uri[0] == 'info:fedora':
                            pid = parsed_fedora_uri[1]
                        else:
                            # Pass the text of the row:
                            pid = row[0]
                        if len(row) == 2:
                            # Set as a literal item
                            items[pid] = row[1]
                        else:
                            # Set as a list of the columns:
                            items[pid] = row[1:]

        return items
    
    def _getText(self, nodelist):
        text = ''
        for node in nodelist:
            if node.nodeType == Node.TEXT_NODE:
                text += node.nodeValue
        return text
    
    def getTrippi(self, query_type, query, lang='itql', format='Sparql',limit='100'):
        # Get Tuples from the risearch servlet running on the Fedora
        # server
        
        query_type = query_type.lower()
        
        if query != '' and (query_type == 'tuples' or query_type == 'triples'):
            queryparams = urllib.urlencode({'type' : query_type, 'lang' : lang, 'format' : format, 'query' : query, 'limit' : limit })
            response = urllib.urlopen( self.server, queryparams).read()
            return response

        # Test for a correct response, return blank if Trippi errors
        # out
        return None
    
    def getTriples(self, query, lang='spo', format='N-Triples', limit='100', offset='0'):
        return self.getTrippi('triples', query, lang, format, limit)
        
    def getTuples(self, query, lang='itql', format='sparql', limit='100', offset='0'):
        return self.getTrippi('tuples', query, lang, format, limit)
        
    def getCount(self, query, lang='spo', query_type='triples'):
        return self.getTrippi(query_type, query, lang=lang, format='count', limit="")

    def doesTripleExist(self, query):
        # Convienience method - lang is 'spo' and relies on my patches to Trippiserver
        count = self.getCount(query)
        
        if count != '0':
            return True
        else:
            return False
            
        """
        Old method:
        
        triple_pattern = re.compile('^<.*?> <.*?> <.*?> .')
        response = self.getTrippi('triples', query, lang="spo", format='N-Triples', limit="10")
        if triple_pattern.match(response):
            return True
        else:
            return False
        """

##  End of Basic RIsearch API functions
################################################################################################

################################################################################################
##  Beginning of convenience functions based on the above API

    def doesPIDExist(self, pid):
        """Test to see if the pid node is the subject of any triples in the triplestore
        # Very much faster than SOAP methods, but is accurate only so far as the 
        # triplestore is accurate"""
        return self.doesTripleExist(query='<info:fedora/'+pid+'> * *')
        
    def getContentType(self, pid):
        query = "select $object from <#ri> where <info:fedora/%s> <info:fedora/fedora-system:def/relations-external#isMemberOf> $object" % (pid)
        linelist = self.getTuples(query, format='csv').split("\n")
        if len(linelist) == 3:
            return linelist[1].split('/')[-1]
        else:
            return False
        
# For UUID <-> tinypid linking
    def resolveTinyPid(self, pid):
        query = "select $object from <#ri> where $object <info:fedora/fedora-system:def/model#label> '" + pid +"'"
        linelist = self.getTuples(query, format='csv').split("\n")
        if len(linelist) == 3:
            return linelist[1].split('/')[-1]
        else:
            return False
        
    def simplifyUUID(self, uuid_pid):
        query = "select $object from <#ri> where <info:fedora/"+uuid_pid+"> <info:fedora/fedora-system:def/model#label> $object"
        linelist = self.getTuples(query, format='csv').split("\n")
        if len(linelist) == 3:
            return linelist[1].split('/')[-1]
        else:
            return False
        
    def getTriplesGraph(self, pid):
        rdf  = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        ore  = Namespace("http://www.openarchives.org/ore/terms/")
        dc  = Namespace("http://purl.org/dc/elements/1.1/")
        dcterms  = Namespace("http://purl.org/dc/terms/")
        owl  = Namespace("http://www.w3.org/2002/07/owl#")
        rel = Namespace("info:fedora/fedora-system:def/relations-external#")
        view = Namespace("info:fedora/fedora-system:def/view#")
        model = Namespace("info:fedora/fedora-system:def/model#")
        rdfs = Namespace("http://www.w3.org/2001/01/rdf-schema#")
        
        bindings = { u"rdf": rdf, u"dc": dc, u"dcterms": dcterms, u'owl':owl, u'ore':ore, u'rel': rel, u'view': view, u'model':model, u'rdfs': rdfs }
        
        g = cg(identifier=pid)
        for prefix in bindings:
            g.bind(prefix, bindings[prefix])

        s = Sink(g)
        
        # NTriples parser
        p  = NTriplesParser(sink=s)

        # Get the (pid, p, o) triples
        query='<info:fedora/'+pid+'> * *'
        ntriples = self.getTriples(query)
        p.parsestring(ntriples)
        
        # Get the (s, p, pid) triples
        query='* * <info:fedora/'+pid+'>'
        ntriples = self.getTriples(query)
        p.parsestring(ntriples)
        
        return g

class Sink(object):
    def __init__(self, g):
        self.length = 0
        self.g = g

    def triple(self, s, p, o):
        self.length += 1
        self.g.add((s,p,o))
