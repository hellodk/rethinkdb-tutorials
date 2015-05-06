import config
import rethinkdb as r


def get_db_connection():
	r.connect(config.RETHINK_DB_SERVER, config.PORT_FOR_CLIENT_DRIVERS).repl()

def create_db(db_name):
	pass

def create_table(table_name):
	r.db('dk').table_create('tv_shows').run()
	r.table('tv_shows').insert({ 'name': 'Star Trek TNG' }).run()

def delete_table(table_name):
	pass

def delete_db(db_name):
	pass

if __name__ == '__main__':
	print 'hello'


