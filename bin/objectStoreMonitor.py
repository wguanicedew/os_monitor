#!/usr/bin/env python

import hashlib
import json
import os
import socket
import sys
import time
import traceback
import urlparse

main_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.join(main_dir, 'lib'))
os.environ["RUCIO_HOME"] = main_dir

from rucio_monitor import record_timer, record_counter
import boto
import boto.s3.connection
from boto.s3.key import Key

class ObjectStoreMonitor:
    def __init__(self):
        self.__current_path = os.path.dirname(os.path.realpath(__file__))
        self.__rse_file = os.path.join(os.path.dirname(self.__current_path), 'etc/rse-accounts.cfg')
        self.__data_file = os.path.join(os.path.dirname(self.__current_path), 'data/HITS.06282451._000321.pool.root.1.6282451-2573318909-3868702156-955-10')
        self.__md5sum = self.getMd5(self.__data_file)
        self.__rses = {}
        self.getObjectStores()
        print self.__rses
        

    def getMd5(self, file):
        hash_md5 = hashlib.md5()
        with open(file, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def loadObjectStores(self):
        with open(self.__rse_file) as json_data:
            accounts = json.load(json_data)
            json_data.close()
        return accounts

    def getObjectStores(self):
        objects = {}
        file = open("/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_ddmendpoints_objectstores.json")
        objectstores = json.load(file)
        for obj in objectstores:
            for proId in objectstores[obj]['rprotocols']:
                if 'activities' in objectstores[obj]['rprotocols'][proId] and 'w' in objectstores[obj]['rprotocols'][proId]['activities']:
                    try:
                        objects[obj] = objects[obj] = {'endpoint': objectstores[obj]['rprotocols'][proId]['endpoint'], 'is_secure': objectstores[obj]['rprotocols'][proId]['settings']['is_secure']}
                    except:
                        print  objectstores[obj]['rprotocols'][proId]
                        print traceback.format_exc()
        rse_accounts = self.loadObjectStores()
        for object in objects:
            if object in rse_accounts:
                objects[object]["access_key"] = rse_accounts[object]["access_key"]
                objects[object]["secret_key"] = rse_accounts[object]["secret_key"]
                self.__rses[object] = objects[object]
        return objects

    def list_hosts(self, hostname, port):
        hosts = []
        socket_hosts = socket.getaddrinfo(hostname, port)
        # print socket_hosts
        for socket_host in socket_hosts:
            if socket_host[4][0] not in hosts and not ':' in socket_host[4][0]:
                hosts.append(socket_host[4][0])
        return hosts

    def create_bucket(self, rse, attributes):
        print "Creating bucket on %s" % (rse)
        access_key = attributes['access_key']
        secret_key = attributes['secret_key']
        is_secure = attributes['is_secure']
        s3path = attributes['endpoint']
        parsed = urlparse.urlparse(s3path)
        scheme = parsed.scheme
        hostname = parsed.netloc.partition(':')[0]
        port = int(parsed.netloc.partition(':')[2]) if parsed.netloc.partition(':')[2] != '' else 0

        try:
                conn = boto.connect_s3(aws_access_key_id = access_key,
                                       aws_secret_access_key = secret_key,
                                       host = hostname,
                                       port = port,
                                       is_secure=is_secure,           # uncommmnt if you are not using ssl
                                       calling_format = boto.s3.connection.OrdinaryCallingFormat(),
                                       )

                bucket = conn.create_bucket("atlas-test-bucket-new")
        except:
            print "Failed to create bucket on %s: %s" % (rse, traceback.format_exc())

    def test_upload_download(self, rse, attributes):
        t_start = time.time()
        access_key = attributes['access_key']
        secret_key = attributes['secret_key']
        is_secure = attributes['is_secure']
        s3path = attributes['endpoint']
        parsed = urlparse.urlparse(s3path)
        scheme = parsed.scheme
        hostname = parsed.netloc.partition(':')[0]
        port = int(parsed.netloc.partition(':')[2]) if parsed.netloc.partition(':')[2] != '' else 0

        balance_hosts = self.list_hosts(hostname, port)

        for balance_host in balance_hosts:
            print "Working on %s: %s" % (rse, balance_host)
            try:
                conn = boto.connect_s3(aws_access_key_id = access_key,
                                       aws_secret_access_key = secret_key,
                                       host = balance_host,
                                       port = port,
                                       is_secure=is_secure,           # uncommmnt if you are not using ssl
                                       calling_format = boto.s3.connection.OrdinaryCallingFormat(),
                                       )

                bucket = conn.get_bucket("atlas-test-bucket-new")

                #upload
                key = Key(bucket)
                key.key = os.path.basename(self.__data_file)
                key.md5 = self.__md5sum
                key.set_metadata("md5", self.__md5sum)
                key.set_contents_from_filename(self.__data_file)
                # print key.key
                # print key.size
                # print "key.md5 " + key.md5
                # print "key.etag " + key.etag.strip('"')

                #download
                key = Key(bucket)
                key.key = os.path.basename(self.__data_file)
                key.get_contents_to_filename("/tmp/%s" % os.path.basename(self.__data_file))
                # print key.size
                # print "key.md5 " + key.md5
                # print "key.etag " + key.etag.strip('"')
                # print "key.get_metadata " + str(key.get_metadata("md5"))

                record_timer("objectstore.upload_download.time.success.%s.%s" % (rse, balance_host.split(".")[-1]), (time.time() - t_start))
                record_timer("objectstore.upload_download.time.success.%s" % (rse), (time.time() - t_start))
                record_counter("objectstore.upload_download.%s.%s.success" % (rse, balance_host.split(".")[-1]))
                record_counter("objectstore.upload_download.%s.success" % (rse))
            except:
                print traceback.format_exc()
                record_counter("objectstore.upload_download.%s.%s.failure" % (rse, balance_host.split(".")[-1]))
                record_counter("objectstore.upload_download.%s.failure" % (rse))
                record_timer("objectstore.upload_download.time.failure.%s.%s" % (rse, balance_host.split(".")[-1]), (time.time() - t_start))
                record_timer("objectstore.upload_download.time.failure.%s" % (rse), (time.time() - t_start))

    def run(self):
        for rse in self.__rses:
            if "AMAZON" in rse:
                continue
            self.create_bucket(rse, self.__rses[rse])
        for i in range(2):
            for rse in self.__rses:
                # if not rse == 'UKI-NORTHGRID-LANCS-HEP_LOGS':
                #     continue
                self.test_upload_download(rse, self.__rses[rse])

if __name__ == "__main__":
    instance = ObjectStoreMonitor()
    instance.run()
