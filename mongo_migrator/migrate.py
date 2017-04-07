#!/usr/bin/python

# -*- coding: utf-8 -*-
from pymongo import MongoClient
from sys import argv, path, stdout

import os
import yaml
import cx_Oracle
import logging
import logging.handlers
import optparse

from mongo_migrator import __version__

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
        self.log_file = None
        self.logger = None
        self.log_on_stdout = True
        self.log_level = 'INFO'

        # Configuration of command line parser.
        self.parser = optparse.OptionParser(
            usage="Usage: %prog [options]",
            version="Version %s" % __version__)

        self.parser.add_option("-c", '--config',
                               action="store",
                               dest='config',
                               help="ABS path of the YAML configuration file.")
        self.parser.add_option("-L", "--loglevel",
                               action="store",
                               dest='log_level',
                               default='INFO',
                               help="%s\n%s" %
                                    ("Define log level between these values: INFO, DEBUG, WARNING, ERROR.",
                                     " (Optional, default is INFO)."))
        self.parser.add_option('-l', '--logfile',
                               action='store',
                               default=None,
                               dest='log_file',
                               help='Log file to use for trace migration operation. (Optional).')
        self.parser.add_option('-q', '--quiet',
                               action='store_true',
                               default=False,
                               dest='quiet',
                               help='Quiet stdout logging.')

    def init_logging(self):

        if self.log_level == 'DEBUG':
            level = logging.DEBUG
        elif self.log_level == 'WARNING':
            level = logging.WARNING
        elif self.log_level == 'ERROR':
            level = logging.ERROR
        else:
            level = logging.INFO

        self.logger = logging.getLogger('mongo-migrator')
        self.logger.setLevel(level)

        formatter = \
            logging.Formatter('%(asctime)s %(levelname)s (%(threadName)-10s) %(message)s')

        if self.log_file:
            handler = logging.handlers.RotatingFileHandler(self.log_file)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        if self.log_on_stdout:
            handlerStdout = logging.StreamHandler(stdout)
            handlerStdout.setFormatter(formatter)
            handlerStdout.setLevel(level)
            self.logger.addHandler(handlerStdout)

    def rows_as_dicts(self, cursor, index):
        """ returns cx_Oracle rows as dicts """
        colnames = []
        elaborate_data = False
        for i in cursor.description:
            if i[0] in self.configuration['tables'][index]['columns']:

                mongo_column = self.configuration['tables'][index]['columns'][i[0]]

                if len(mongo_column.split()) > 1 and self.import_pkg:
                    elaborate_data = True

                self.logger.debug('For column %s use mongo_column %s' %
                                  (i[0],
                                   mongo_column))
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
                                self.logger.debug('Skip column %s' % split[0])
                            else:
                                new_row.append(res)
                                self.logger.debug('For mongo_column %s => %s' % (split[0], res))

                    else:
                        new_row.append(c)
                        self.logger.debug('For mongo_column %s => %s' % (split[0], c))

                    if not skip_column:
                        elabnames.append(split[0])
                    i += 1

                self.logger.debug(
                    '\n%s\nMigrate row [%s] => [%s]\n%s',
                    "--------------------------------------------------------------------------------------",
                    row, new_row,
                    "--------------------------------------------------------------------------------------"
                )

                yield dict(zip(elabnames, new_row))
            else:
                yield dict(zip(colnames, row))

    def read_from_oracle(self, index):

        table_name = self.configuration['tables'][index]['table_name']
        query = self.configuration['tables'][index]['query']

        self.logger.info('\n%s\nFor table %s executing query:\n%s\n%s',
                         "--------------------------------------------------------------------------------------",
                         table_name, query,
                         "--------------------------------------------------------------------------------------")

        oracle_cursor = self.oracleConnection.cursor()
        oracle_cursor.execute(self.configuration['tables'][index]['query'])

        return self.rows_as_dicts(oracle_cursor, index)

    def load_configuration(self, filename):
        """ loads yml configuration file"""

        f = open(filename)

        self.logger.debug('\n%s\nParse of configuration file %s\n%s',
                          "--------------------------------------------------------------------------------------",
                          filename,
                          "--------------------------------------------------------------------------------------")
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
        n_rec = 0
        for i in data:
            db[self.configuration['tables'][index]['collectionName']].insert(i)
            n_rec += 1

        self.logger.info('\n%s\nNumber of record inserted on collection %s: %s\n%s',
                         "--------------------------------------------------------------------------------------",
                         self.configuration['tables'][index]['collectionName'],
                         n_rec,
                         "--------------------------------------------------------------------------------------")

    def main(self):

        # Parse command line
        (options, args) = self.parser.parse_args()

        if options.config is None:
            self.parser.error('Missing mandatory configuration file option. Use --help for help message')

        if options.quiet:
            self.log_on_stdout = False

        if options.log_file:
            self.log_file = options.log_file

        if options.log_level:
            self.log_level = options.log_level

        self.init_logging()
        self.configuration = self.load_configuration(options.config)

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
            self.logger.info("\n%s\nStart elaboration of the table %s\n%s",
                             "======================================================================================",
                             self.configuration['tables'][index]['table_name'],
                             "--------------------------------------------------------------------------------------")
            data = self.read_from_oracle(index)
            self.logger.debug('\n%s\n%s\n%s',
                              "--------------------------------------------------------------------------------------",
                              "Elaborated data: %s" % data,
                              "--------------------------------------------------------------------------------------")
            self.insert_mongo(data, index)
            self.logger.info("\n%s\nEnd of elaboration of the table %s.\n%s",
                             "--------------------------------------------------------------------------------------",
                             self.configuration['tables'][index]['table_name'],
                             "======================================================================================")

        exit(0)


def main():
    m = MongoMigrator()
    m.main()

if __name__ == "__main__":
    main()
