import config
import rethinkdb as r


def connect_db():
	r.connect('localhost', 28015).repl()
	r.db('test').table_create('tv_shows').run()
	r.table('tv_shows').insert({ 'name': 'Star Trek TNG' }).run()


def create_db(db_name):
	

def create_table(table_name):