#!/usr/bin/python

## to use fedora objects in python console, run:
## python -i test.py

from pythonFedoraCommons import fedoraClient,risearch

client = fedoraClient.ClientFactory()
fedora = client.getClient("http://wilson:7080/fedora", "fedoraAdmin", "fedoraAdmin", "2.2")
ri = risearch.Risearch("http://wilson:6080/fedora")
