import time
import uuid

from pymongo import MongoClient


from indexer.schema import header, group, stream, doctypes, dtypes
from p4p import Type, Value
from p4p.nt import NTMultiChannel, NTTable

from p4p.rpc import rpc, quickRPCServer, WorkQueue
# TODO: Add alarm status and severity of failed requests

class DixrRO:
    def __init__(self, config):
        self.config = config

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, config):
        for key in ['host', 'port', 'database']:
            if key not in config:
                raise KeyError('Required fields missing')
        self._config = config
        
    @property
    def conn(self):
        return MongoClient(host=self.config['host'],
                           port=self.config['port'])
        
    @property
    def db(self):
        return self.conn[self.config['database']]

    def query(self, doctype, **kwargs):
        # TODO: Use a generator instead of return
        print('Here are', kwargs)
        cursor = self.db[doctype].find(kwargs)
        result = list(cursor)
        normed = self._normalize_results(doctype=doctype,
                                         results=result)
        return self._bson2mtch(doctype=doctype,
                               docs=normed)

    def _pv2bson(self, doc, doctype):
        """Convert from pvdata Value to python dict """
        lookup = {'i': int, 'd': float, 's': str, 'ai': int, 'ad': float, 'as': str}
        bson = {}
        for entry in dtypes[doctype]:
            # TODO: A more sophisticated way to fetch array
            bson[entry[0]] = lookup[entry[1]](doc.get(entry[0]))
        return bson

    def _bson2pv(self, doctype, doc):
        """Given a dictionary, return a pvdata structure"""
        T = Type(dtypes[doctype])
        return Value(T, doc)

    def _bson2mtch(self, doctype, docs):
        # TODO: Exception handling if wrong doc format provided
        table_template = NTTable.buildType(columns=dtypes[doctype])    
        table =  Value(table_template,{'value': docs, 
                                       'timeStamp.secondsPastEpoch': time.time(),
                                        'descriptor': doctype})
        table.labels = self.keys
        return table
    
    def _normalize_results(self, doctype, results):
        keys = [i[0] for i in dtypes[doctype]]
        self.keys = keys
        tmp_dict = {}
        for k in keys:
            tmp_dict[k] = []
        for result in results:
                for k in keys:
                    tmp_dict[k].append(result[k])
        return tmp_dict
    

class Dixr(DixrRO):
   def insert(self, doctype, contents):
       bson = self._pv2bson(doc=contents, doctype=doctype)
       self.db[doctype].insert_one(bson)

config = dict(host='localhost', port=27017, database='dixrtest')

rw = Dixr(config=config)

conn = rw
class DixrHeaderHandler(object):
    @rpc(NTTable.buildType(columns=dtypes['header']))
    def get_header_given_uid(self, uid):
        return conn.query(doctype='header', uid=uid)
        
querycall = DixrHeaderHandler()
quickRPCServer(provider="DixrHeaderHandler",
               prefix="pv:call:",
               target=querycall)