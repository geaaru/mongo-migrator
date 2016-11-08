#!/usr/bin/python

# -*- coding: utf-8 -*-
from pymongo import MongoClient
from sys import argv, path

import os
import yaml
import cx_Oracle

os.environ["NLS_LANG"] = "AMERICAN_AMERICA.UTF8"

def rows_as_dicts(cursor,configuration,index, import_pkg=None):
    """ returns cx_Oracle rows as dicts """
    colnames = []
    elaborate_data = False
    for i in cursor.description:
        if i[0] in configuration['tables'][index]['columns']:

            mongo_column = configuration['tables'][index]['columns'][i[0]]

            if len(mongo_column.split()) > 1 and import_pkg:
                elaborate_data = True

            colnames.append(mongo_column)

    for row in cursor:
        i=0
        if elaborate_data:
            elabnames=[]
            new_row=[]
            for c in row:
                #print(c)
                split = colnames[i].split()
                if len(split) > 1:
                    if split[1] == 'function':

                        if not import_pkg:
                            raise Exception('No import script available!')

                        method = getattr(import_pkg, split[2])
                        if not method:
                            raise Exception('Function \'%s\' not found' % split[2])

                        new_row.append(method(c, mongo_column=split[0],
                                              row=row, configuration=configuration))
                else:
                    new_row.append(c)

                elabnames.append(split[0])
                i += 1

            yield dict(zip(elabnames, new_row))
        else:
            yield dict(zip(colnames, row))

def readFromOracle(configuration,index, import_pkg=None):

    oracleConnection = cx_Oracle.connect(configuration['oracle_configuration']['username'] + "/" +
                                         configuration['oracle_configuration']['password'] + "@" +
                                         configuration['oracle_configuration']['dsn'])

    table_name = configuration['tables'][index]['table_name']
    query = configuration['tables'][index]['query']
    print('Retrieve data for table', table_name, 'with query', query)

    oracleCursor = oracleConnection.cursor()
    oracleCursor.execute(configuration['tables'][index]['query'])

    return rows_as_dicts(oracleCursor,configuration,index,import_pkg=import_pkg)

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

def importScript(importFile):

    path.insert(0, os.path.dirname(importFile))
    f = os.path.basename(importFile)
    f = f.replace('.py', '')
    return __import__(f)

def main():
    configuration = loadConfiguration()
    print(configuration)
    pkg=None
    if 'import_functions' in configuration and 'pyfile' in configuration['import_functions']:
        pyfile = configuration['import_functions']['pyfile']
        pkg = importScript(pyfile)

    for index in range(len(configuration['tables'])):
        print("Table to be migrated :" + configuration['tables'][index]['table_name'])
        data   = readFromOracle(configuration,index, import_pkg=pkg)
        result = insertMongo(data,configuration,index)

if __name__ == "__main__":
    main()
