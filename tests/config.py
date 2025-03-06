from libb import Setting

Setting.unlock()

postgres = Setting()
postgres.drivername='postgres'
postgres.hostname='localhost'
postgres.username='postgres'
postgres.password='postgres'
postgres.database='test_db'
postgres.port=5432
postgres.timeout=30

sqlite = Setting()
sqlite.drivername='sqlite'
sqlite.database='database.db'

sqlserver = Setting()
sqlserver.drivername='sqlserver'
sqlserver.hostname='localhost'
sqlserver.username='sa'
sqlserver.password='StrongPassword123!'
sqlserver.database='master'
sqlserver.port=1433
sqlserver.timeout=30

Setting.lock()
