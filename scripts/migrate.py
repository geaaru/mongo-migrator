#!/usr/bin/python
# -*- coding: utf-8 -*-
from pymongo import MongoClient
from sys import argv

import os
import yaml
import cx_Oracle

os.environ["NLS_LANG"] = "AMERICAN_AMERICA.UTF8"

def rows_as_dicts(cursor,configuration,index):
    """ returns cx_Oracle rows as dicts """
    colnames = []
    for i in cursor.description:
        if i[0] in configuration['tables'][index]['columns']:
            colnames.append(configuration['tables'][index]['columns'][i[0]])

    for row in cursor:
        yield dict(zip(colnames, row))

def readFromOracle(configuration,index):

    oracleConnection = cx_Oracle.connect(configuration['oracle_configuration']['username'] + "/" +
                                         configuration['oracle_configuration']['password'] + "@" +
                                         configuration['oracle_configuration']['dsn'])

    table_name = configuration['tables'][index]['table_name']
    query = configuration['tables'][index]['query']
    print('Retrieve data for table', table_name, 'with query', query)

    oracleCursor = oracleConnection.cursor()
    oracleCursor.execute(configuration['tables'][index]['query'])

    return rows_as_dicts(oracleCursor,configuration,index)

def insertMongo(data,configuration,index):
    mongoClient = MongoClient('mongodb://%s:%s@%s:%d/%s?authSource=%s' % (
                              configuration['mongo_server']['username'],
                              configuration['mongo_server']['password'],
                              configuration['mongo_server']['host'],
                              configuration['mongo_server']['port'],
                              configuration['mongo_server']['db'],
                              configuration['mongo_server']['auth_source']))
    db = mongoClient[configuration['mongo_server']['db']]
    for i in data:
        db[configuration['tables'][index]['collectionName']].insert(i)

    print("Number of records migrated to Mongo :",
          db[configuration['tables'][index]['collectionName']].count())

def loadConfiguration():
    """ loads yml configuration file"""
    if len(argv) != 2:
        print("Missing config file argoment")
        print('Use ', argv[0], ' config.conf')
        exit(1)

    filename = argv[1]
    f = open(filename)
    configuration = yaml.safe_load(f)
    f.close()

    #print(yaml.dump(configuration))

    return configuration

def main():
    configuration = loadConfiguration()
    print(configuration)
    for index in range(len(configuration['tables'])):
        print("Table to be migrated :" + configuration['tables'][index]['table_name'])
        data   = readFromOracle(configuration,index)
        result = insertMongo(data,configuration,index)

if __name__ == "__main__":
    main()
