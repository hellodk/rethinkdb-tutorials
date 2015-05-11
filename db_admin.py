import config
import rethinkdb as r

from rethinkdb.errors import RqlRuntimeError

def get_db_connection():
    try:
        #conn = r.connect(host='localhost', port=28015)
        global connection
        connection = r.connect(config.RETHINK_DB_INSTANCE_HOST, config.DRIVER_PORT).repl()
        print 'db connection created ',connection
    except Exception as ex:
        print 'Connection Establishment faield ', ex.message()
    # '''    r.connect({ host: 'localhost',
    #         port: 28015,
    #         db: 'marvel',
    #         authKey: 'hunter2' },
    #       function(err, conn) { ... })'''
    return connection

def reconnect():
    try:
        connection.reconnect(noreply_wait=False)
    except Exception as ex:
        print 'exceptin occurred ',ex
    print 'Reconnected'

def close_connection():
    try:
        connection.close()
        print 'connection closed'
    except Exception as ex:
        print 'Issue while closing the connection ',ex

def create_db(db_name):
    try:
        r.db_create(db_name).run()
    except RqlRuntimeError as rql:
        print 'Failed to create the database ', rql
    except Exception as ex:
        print 'Exception occurred ' , ex

def list_db():
    #get_db_connection()
    print r.db_list().run(connection)

def change_db(db_name):
    try:
        connection.use(db_name)
    except Exception as ex:
        print 'Exception caused',ex

def create_table(db_name,table_name):
    try:
        r.db(db_name).table_create(table_name).run()
        print 'Table created'
    except Exception as ex:
        print 'Caught in exception',ex
        r.table(table_name).insert(r.expr({'age': 26, 'name': 'Michel'}))
        
        #r.table(table_name).insert({'age': 26, 'name': 'Michel'}).run()

    print 'Exception in writing to table'

def delete_table(table_name):
    pass

def delete_db(db_name):
    pass

if __name__ == '__main__':
    print 'hello'

    get_db_connection()
    print 'db connection established'
    create_db(config.DATABASE)
    #print config.DATABASE, ' db created'
    print 'Listing existing database \n', list_db()
    #create_table('dk','phn')
    close_connection()
    reconnect()
    create_db('rohit')
    change_db('rohit')
    create_table('rohit','dk1')

'''
r.table('dk.phn').insert({"age": 22,"name": "Miachel"}).run()
r.table("posts").insert({"name": "Michel","age": 26}).run()
'''