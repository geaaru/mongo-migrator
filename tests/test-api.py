# Author: Geaaru <geaaru@gmail.com>
#

from mongo_migrator.migrate import MongoMigrator

configuration = [
    {
        'import_functions': {
            'pyfile': 'example_function1'
        },
        'oracle_configuration': {
            'username': 'username_of_db',
            'password': 'password_of_db_user',
            'dns': 'tnsname_alias'
        },
        'mongo_server': {
            'username': 'mongo_user',
            'password': 'mongo_pwd',
            'host': 'mongo_host',
            'db': 'mongo_db',
            'auth_source': 'mongo_auth_source',
            'port': 'mongo_port'
        },
        'tables': [
            {
                'table_name': 'dbuser.table_name',
                'query': 'SELECT COLUMN1, COLUMN2 FROM dbuser.table_name',
                'collectionName': 'mongo_target_collection',
                'columns': {
                    'COLUMN1': 'column1',
                    'COLUMN2': 'column2 function myscript_function'
                }
            },
        ]
    }
]

def testApi1():

    global configuration

    migrator = MongoMigrator()
    # Set script directory to avoid check of configfile attribute
    migrator.script_dir = './examples/'

    migrator.configuration = configuration

    # Migrate data
    migrator.migrate()


def testApi2():

    migrator = MongoMigrator()

    # Set script directory to avoid check of configfile attribute
    migrator.script_dir = './examples/'
    # Load configuration from file
    migrator.load_configuration('../examples/migration_config.yml')

    # Do some changes to query
    old_query = migrator.configuration['tables'][0]['query']

    migrator.configuration['tables'][0]['query'] = old_query.replace('PLACEHOLDER', 'STRING')

    # Migrate data
    migrator.migrate()


#testApi1()
#testApi2()

# vim: ts=4 sw=4 expandtab