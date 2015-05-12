import db_admin
import config

import rethinkdb as r

connection = db_admin.get_db_connection()
print db_admin.is_connection_open(connection)
print 'db_conection established'

#Read values based on the existing phone numbers

# cur = r.table("sample").run()
# for i in cur:
#     print '\n',cur

print 'The number of records is ',db_admin.get_number_of_records('ndnc','sample')

#print 'data is : ', r.table('sample').filter({"Phone Number":9827320232}).run()
phone_number = '9827320232'
print db_admin.get_records(config.TABLE,phone_number,connection)
#print r.table(config.DATABASE).get(phone_number).run(connection)

#print r.table(config.TABLE).get('9827320232').run(connection)