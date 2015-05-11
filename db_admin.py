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
	r.db_create(db_name).run()

def create_table(db_name,table_name):
	try:
		r.db(db_name).table_create(table_name).run()
		print 'Table created'
	except:
		print 'Caught in exception'
        r.table(table_name).insert(r.expr({'age': 26, 'name': 'Michel'}))
		
		#r.table(table_name).insert({'age': 26, 'name': 'Michel'}).run()

	print 'Exception in writing to table'

def delete_table(table_name):
	pass

def delete_db(db_name):
	pass

if __name__ == '__main__':
	print 'hello'
'''	get_db_connection()
	create_table('dk','phn')
'''

'''
r.table('dk.phn').insert({"age": 22,"name": "Miachel"}).run()

r.table("posts").insert({"name": "Michel","age": 26}).run()


'''
