from typing import Collection
from pymongo import MongoClient
import sys
import json
import argparse
import configparser
import os
from datetime import datetime

class Mongo:
    def __init__(self):
        
        # Config
        self.basedir = os.path.dirname(os.path.abspath(__file__))

        # config MongoDB
        conf = configparser.ConfigParser()
        conf.read('config.ini')
        mongo_user = conf['mongodb']['USER']
        mongo_pass = conf['mongodb']['PASSWORD']
        mongo_host = conf['mongodb']['HOST']
        mongo_port = conf['mongodb']['PORT']
        uri = f'mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}/'
        client = MongoClient(uri)
        self.db = client['salt_invent']

    # Create a dump JSON file for each collection
    def dump(self):
        cols = self.db.list_collection_names()
        for col in cols:
            data = self.db[col].find()
            data_list = []
            dump_file_name = f'{ col }.json'
            dump_file = open(dump_file_name,'w')
            for i in data:
                i.pop('_id')
                i.pop('datetime')
                data_list.append(i)
            dump_file.write(str(json.dumps(data_list)))
            dump_file.close()    

    # Restore a mongo database
    def restore(self):
        try:
            files = [ x for x in os.listdir(self.basedir) if x.endswith('.json') ]
            for file in files:
                data = open('{}/{}'.format(self.basedir,file)).read()
                collection = json.loads(data)
                collection_name = file.replace('.json','')
                for doc in collection:
                    doc['datetime'] = datetime.now()
                    coll = self.db[collection_name]
                    coll.insert_one(doc)
        except Exception as err:
            print('Error: {}'.format(err))

if __name__ == '__main__':
    parse = argparse.ArgumentParser("Dummy Python script to Dump and Restore data")
    parse.add_argument('-d', '--dump' , help='Create a database dump', action='store_true')
    parse.add_argument('-r', '--restore' , help='Restore a database', action='store_true')
    parse.add_argument('-D', '--database' , help='Database name')

    args = vars(parse.parse_args())

    if len(sys.argv) == 1:
        print(parse.format_help())
        sys.exit(0)
    elif args['dump'] and args['restore']:
        print('You need choise only one action: Dump or Restore')
        sys.exit(1)
    elif args['dump']:
        m = Mongo()
        dump = m.dump()
    elif args['restore']:
        m = Mongo()
        restore = m.restore()
        

    
