#!/usr/bin/python

# -*- coding: utf-8 -*-
from pymongo import MongoClient
from sys import argv, path

import os
import yaml
import cx_Oracle

os.environ["NLS_LANG"] = "AMERICAN_AMERICA.UTF8"

class Operation:

    def __init__(self):
        self.skip_column = False

class MongoMigrator:

    def __init__(self):
        self.mongoClient = None
        self.oracleConnection = None
        self.configuration = None
        self.import_pkg = None

    def rows_as_dicts(self, cursor, index):
        """ returns cx_Oracle rows as dicts """
        colnames = []
        elaborate_data = False
        for i in cursor.description:
            if i[0] in self.configuration['tables'][index]['columns']:

                mongo_column = self.configuration['tables'][index]['columns'][i[0]]

                if len(mongo_column.split()) > 1 and self.import_pkg:
                    elaborate_data = True

                colnames.append(mongo_column)

        for row in cursor:
            i = 0
            if elaborate_data:
                elabnames = []
                new_row = []
                for c in row:
                    # print(c)
                    skip_column = False
                    split = colnames[i].split()
                    if len(split) > 1:
                        if split[1] == 'function':

                            if not self.import_pkg:
                                raise Exception('No import script available!')

                            method = getattr(self.import_pkg, split[2])
                            if not method:
                                raise Exception('Function \'%s\' not found' % split[2])
                            op = Operation()
                            res = method(c, mongo_column=split[0],
                                         row=row,
                                         configuration=self.configuration,
                                         mongoClient=self.mongoClient,
                                         oracleConnection=self.oracleConnection,
                                         operator=op)
                            if op.skip_column:
                                skip_column = True
                            else:
                                new_row.append(res)

                    else:
                        new_row.append(c)

                    if not skip_column:
                        elabnames.append(split[0])
                    i += 1

                yield dict(zip(elabnames, new_row))
            else:
                yield dict(zip(colnames, row))

    def read_from_oracle(self, index):

        table_name = self.configuration['tables'][index]['table_name']
        query = self.configuration['tables'][index]['query']
        print('Retrieve data for table', table_name, 'with query', query)

        oracle_cursor = self.oracleConnection.cursor()
        oracle_cursor.execute(self.configuration['tables'][index]['query'])

        return self.rows_as_dicts(oracle_cursor, index)

    @staticmethod
    def load_configuration(filename):
        """ loads yml configuration file"""

        f = open(filename)
        configuration = yaml.safe_load(f)
        f.close()

        return configuration

    @staticmethod
    def import_script(import_file):

        path.insert(0, os.path.dirname(import_file))
        f = os.path.basename(import_file)
        f = f.replace('.py', '')
        return __import__(f)

    def insert_mongo(self, data, index):
        db = self.mongoClient[self.configuration['mongo_server']['db']]
        for i in data:
            db[self.configuration['tables'][index]['collectionName']].insert(i)

        print("Number of records migrated to Mongo :",
              db[self.configuration['tables'][index]['collectionName']].count())

    def main(self):

        if len(argv) != 2:
            print("Missing config file argoment")
            print('Use ', argv[0], ' config.conf')
            exit(1)

        self.configuration = self.load_configuration(argv[1])

        # print(configuration)
        if 'import_functions' in self.configuration and 'pyfile' in self.configuration['import_functions']:
            pyfile = self.configuration['import_functions']['pyfile']
            self.import_pkg = self.import_script(pyfile)

        # Create connection to oracle
        self.oracleConnection = cx_Oracle.connect(self.configuration['oracle_configuration']['username'] + "/" +
                                                  self.configuration['oracle_configuration']['password'] + "@" +
                                                  self.configuration['oracle_configuration']['dsn'])

        self.mongoClient = MongoClient('mongodb://%s:%s@%s:%d/%s?authSource=%s' % (
                                       self.configuration['mongo_server']['username'],
                                       self.configuration['mongo_server']['password'],
                                       self.configuration['mongo_server']['host'],
                                       self.configuration['mongo_server']['port'],
                                       self.configuration['mongo_server']['db'],
                                       self.configuration['mongo_server']['auth_source']))
        # TODO: add check of the connections

        # Execute migration for any table of the configuration file.
        for index in range(len(self.configuration['tables'])):
            print("Table to be migrated :" + self.configuration['tables'][index]['table_name'])
            data = self.read_from_oracle(index)
            self.insert_mongo(data, index)

        exit(0)


def main():
    m = MongoMigrator()
    m.main()

if __name__ == "__main__":
    main()
