import config
import rethinkdb as r

from rethinkdb.errors import RqlRuntimeError

def get_db_connection(RETHINK_DB_INSTANCE_HOST='localhost',DRIVER_PORT=28015,DATABASE='test'):
    try:
        #conn = r.connect(host='localhost', port=28015)
        global connection
        connection = r.connect(config.RETHINK_DB_INSTANCE_HOST, config.DRIVER_PORT,config.DATABASE).repl()
    except Exception as ex:
        print 'Connection Establishment failed ', ex.message()
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

def is_connection_open(connection):
    return connection.is_open()

def close_connection():
    try:
        connection.close()
        print 'connection closed ',connection
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

def get_connection_db(connection):
    return conection.db

def change_db(db_name):
    try:
        connection.use(db_name)
    except Exception as ex:
        print 'Exception caused',ex

def get_number_of_records(db_name,table_name):
    return r.table(table_name).count().run()

def get_records(table_name,phone_number,connection):
    return r.table(table_name).get(phone_number).run(connection)

def create_table(db_name,table_name):
    try:
        r.db(db_name).table_create(table_name).run()
        print 'Table created, now adding values '
        r.table(table_name).insert(r.expr({'age': 26, 'name': 'Michel'}))
    except Exception as ex:
        print 'Caught in exception ',ex
        
        #r.table(table_name).insert({'age': 26, 'name': 'Michel'}).run()

def insert_values():
    r.table("phone").insert({
   "user_id": "f62255a8259f",
   "age": 30,
   "name": "Peter"
})
    print 'values inserted'

def delete_table(table_name):
    pass

def delete_db(db_name):
    pass

if __name__ == '__main__':
    get_db_connection()
    #print 'db connection established'
    create_db(config.DATABASE)
    print config.DATABASE, ' db created'
    print 'Listing existing database \n', list_db()
    create_table('ndnc','phone')
    close_connection()
    reconnect()
    create_db('ndnc')
    change_db('ndnc')
    create_table('ndnc','phone')
    insert_values()

# for i in r.table('phone').run(connection):
#     print i

#print is_connection_open()

'''
r.table('ndnc.phone').insert({"age": 22,"name": "Miachel"}).run()
r.table("posts").insert({"name": "Michel","age": 26}).run()
'''