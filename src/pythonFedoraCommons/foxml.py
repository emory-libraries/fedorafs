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
from xml import xpath
from xml.dom.ext import PrettyPrint

class Foxml(object):
    foxml_template = u"""<?xml version="1.0" encoding="UTF-8"?>
<foxml:digitalObject xmlns:rel="info:fedora/fedora-system:def/relations-external#" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:foxml="info:fedora/fedora-system:def/foxml#"
           xmlns:fedoraxsi="http://www.w3.org/2001/XMLSchema-instance"
           xmlns:audit="info:fedora/fedora-system:def/audit#"
           fedoraxsi:schemaLocation="info:fedora/fedora-system:def/foxml# http://www.fedora.info/definitions/1/0/foxml1-0.xsd"
           PID="collection:super">
    <foxml:objectProperties>
        <foxml:property NAME="http://www.w3.org/1999/02/22-rdf-syntax-ns#type" VALUE="FedoraObject"/>
        <foxml:property NAME="info:fedora/fedora-system:def/model#state" VALUE="Active"/>
        <foxml:property NAME="info:fedora/fedora-system:def/model#label" VALUE="Empty"/>
        <foxml:property NAME="info:fedora/fedora-system:def/model#ownerId" VALUE="042c887936d9d03f0db84bfc3e4db37d"/>
        <foxml:property NAME="info:fedora/fedora-system:def/model#createdDate" VALUE="2007-07-17T15:10:51.885Z"/>
        <foxml:property NAME="info:fedora/fedora-system:def/view#lastModifiedDate" VALUE="2007-07-17T15:11:20.291Z"/>
        <foxml:property NAME="info:fedora/fedora-system:def/model#contentModel" VALUE="collection"/>
    </foxml:objectProperties>
</foxml:digitalObject>"""

    foxml_properties = {}
    foxml_properties['ownerId'] = 'info:fedora/fedora-system:def/model#ownerId'
    foxml_properties['contentModel'] = 'info:fedora/fedora-system:def/model#contentModel'
    foxml_properties['createdDate'] = 'info:fedora/fedora-system:def/model#createdDate'
    foxml_properties['state'] = 'info:fedora/fedora-system:def/model#state'
    foxml_properties['label'] = 'info:fedora/fedora-system:def/model#label'

    def __init__(self):
        self.doc = minidom.parseString(Foxml.foxml_template)
        self.ctx = xpath.CreateContext(self.doc)
        self.ctx.setNamespaces({u'foxml':"info:fedora/fedora-system:def/foxml#",
                                u'oai_dc':"http://purl.org/dc/elements/1.1/",
                                u'mods':"http://www.loc.gov/mods/v3",
                                u'fedoraxsi':"http://www.w3.org/2001/XMLSchema-instance",
                                u'audit':"info:fedora/fedora-system:def/audit#",
                                u'rel':"info:fedora/fedora-system:def/relations-external#",
                                u'rdf':"http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                                })

    def setProperty(self, foxml_name, value):
        attribute_name = Foxml.foxml_properties.get(foxml_name, None)

        if attribute_name == None:
            return False

        response = False
        for tag in self.doc.documentElement.getElementsByTagName('foxml:property'):
            if tag.getAttribute('NAME') == attribute_name:
                tag.setAttribute('VALUE',value)
                response = True

        return response

    def setPID(self, pid):
        response = False
        self.doc.documentElement.setAttribute('PID',pid)
        return response


    def toString(self):
        return self.doc.toxml()
