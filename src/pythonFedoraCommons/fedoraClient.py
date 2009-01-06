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

import fedoraClient20, fedoraClient22, fedoraClient30

class ClientFactory(object):
    def __init__(self):
        self.supported_versions = ['2.0','2.2','3.0']
        
    def getClient(self,serverurl='http://localhost:8080/fedora', username='fedoraAdmin', password='fedoraAdmin', version="3.0"):
        if version=="2.2":
            return fedoraClient22.FedoraClient(serverurl=serverurl, username=username, password=password, version="2.2")
        elif version=="2.0":
            return fedoraClient20.FedoraClient(serverurl=serverurl, username=username, password=password, version="2.0")
        elif version=="3.0":
            return fedoraClient30.FedoraClient(server=serverurl, username=username, password=password, version="3.0")
        else:
            raise 'FedoraClient supports APIs for Fedora versions %s only' % (self.supported_versions)
