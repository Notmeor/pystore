
import warnings

import pymongo
from gridfs import GridFS
from bson import ObjectId
from pystore.serializer import serializer

import time, functools


def timeit(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        t0_ = time.time()
        ret = func(*args, **kwargs)
        print('%s in %.6f secs' % (
            func.__name__, time.time() - t0_))
        return ret
    return wrapper


class MongoStore(object):
    
    config_settings = {}
    
    def __init__(self, uri=None, db_name=None):

        if uri is None:
            db_name = self.config_settings['name'] 
            mongo_host = self.config_settings['mongo_host']
            username = self.config_settings['username']
            password = self.config_settings['password']
            
            self.db = pymongo.MongoClient(mongo_host)[db_name]
            self.db.authenticate(username, password)
        else:
            if db_name is None:
                raise Exception('Must provide target db name')
            self.db = pymongo.MongoClient(uri)[db_name]
        
        self.fs = GridFS(self.db)
        
    def write(self, name, df, metadata='', upsert=True):
        
        if upsert:
            self.delete(name)
            
        if name in self.fs.list():
            warnings.warn(
                'filename `{}` already exists, nothing inserted'.format(name))
            return 
                            
        return self.fs.put(
            serializer.serialize(df),
            filename=name,
            metadata=metadata
        )
    
    def delete(self, name):
        doc = self.db['fs.files'].find_one(
            {'filename': name})
        if doc:
            _id = doc.get('_id')
            self.fs.delete(_id)
        
    def read(self, name):

        def _read(name):
            return self.fs.find_one(
                {'filename': name}).read()

        sr = _read(name)
        return serializer.deserialize(sr)
    
    def read_metadata(self, name):
        return self.db['fs.files'].find_one(
            {'filename': name}).get('metadata')
    
    def list(self):
        return self.fs.list()
    
    def __enter__(self):
        return self
    
    def __exit__(self, et, ev, tb):
        self.close()
    
    def close(self):
        self.db.client.close()
         
