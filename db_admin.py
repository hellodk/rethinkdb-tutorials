import config
import rethinkdb as r
import time
from rethinkdb.errors import RqlRuntimeError

connection = None


def get_db_connection(RETHINK_DB_INSTANCE_HOST='localhost',
                      DRIVER_PORT=28015, DATABASE='sample'):
    try:
        global connection
        connection = r.connect(
            config.RETHINK_DB_INSTANCE_HOST, config.DRIVER_PORT,
            config.DATABASE).repl()
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
        # if !(is_connection_open(connection)):
        connection.reconnect(noreply_wait=False)
    except Exception as ex:
        print 'exception occurred ', ex
    print 'Reconnected'


def is_connection_open(connection):
    return connection.is_open()


def close_connection():
    try:
        connection.close()
        print 'connection closed ', connection
    except Exception as ex:
        print 'Issue while closing the connection ', ex


def create_db(db_name):
    try:
        r.db_create(db_name).run()
    except RqlRuntimeError as rql:
        print 'Failed to create the database ', rql
    except Exception as ex:
        print 'Exception occurred ', ex


def list_db():
    # get_db_connection()
    print r.db_list().run(connection)


def get_connection_db(connection):
    return connection.db


def change_db(db_name):
    try:
        connection.use(db_name)
    except Exception as ex:
        print 'Exception caused', ex


def get_number_of_records(db_name, table_name):
    return r.table(table_name).count().run()


def get_records(table_name=config.TABLE,
                phone_number=config.PHONE_NUMBER, connection=connection):
    # get_db_connection()
    return r.table(table_name).get(phone_number).run(connection)


def create_table(db_name, table_name):
    try:
        r.db(db_name).table_create(table_name).run()
        print 'Table created, now adding values '
        r.table(table_name).insert(r.expr({'age': 26, 'name': 'Michel'}))
        r.table("last_upload_time").insert(
            [{"last_upload_time": int(time.time())}]).run()
        r.table("update_times").insert(
            [{"update_times": time.ctime()}]).run()
    except Exception as ex:
        print 'Caught in exception ', ex


def insert_values():
    r.table("phone").insert({
        "user_id": "f62255a8259f",
        "age": 30,
        "name": "Peter"
    })
    r.table("last_upload_time").insert(
        [{"last_upload_time": int(time.time())}]).run()
    print 'values inserted'


def delete_table(table_name):
    pass


def delete_db(db_name):
    pass

if __name__ == '__main__':
    get_db_connection()
    # print 'db connection established'
    # create_db(config.DATABASE)
    # print config.DATABASE, ' db created'
    # print 'Listing existing database \n', list_db()
    # create_table('ndnc','phone')
    # close_connection()
    # reconnect()
    # create_db('ndnc')
    # change_db('ndnc')
    # create_table('ndnc','phone')
    insert_values()
    print get_number_of_records("ndnc",
                                "last_upload_time")

# for i in r.table('phone').run(connection):
#     print i

# print is_connection_open()

'''
r.table('ndnc.phone').insert({"age": 22,"name": "Miachel"}).run()
r.table("posts").insert({"name": "Michel","age": 26}).run()
'''
