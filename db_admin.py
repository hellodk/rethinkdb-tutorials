import config
import rethinkdb as r


def get_db_connection():
	try:
		r.connect(config.RETHINK_DB_INSTANCE_HOST, config.DRIVER_PORT).repl()
		print 'db connection created'
	except ex:
		print 'Connection Establishment faield ', ex

def close_connection():
	pass

def create_db(db_name):
	pass

def create_table(table_name):
	r.db('dk').table_create('ta).run()
	r.table('ta).insert({ 'name': 'Star Trek TNG' }).run()

def delete_table(table_name):
	pass

def delete_db(db_name):
	pass

if __name__ == '__main__':
	print 'hello'
	get_db_connection()


