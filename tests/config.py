from database.options import iterdict_data_loader

from libb import Setting

Setting.unlock()

postgresql = Setting()
postgresql.drivername='postgresql'
postgresql.hostname='localhost'
postgresql.username='postgres'
postgresql.password='postgres'
postgresql.database='test_db'
postgresql.port=5432
postgresql.timeout=30
postgresql.data_loader=iterdict_data_loader
postgresql.use_pool=False
postgresql.pool_max_connections=1
postgresql.pool_max_idle_time=600
postgresql.pool_wait_timeout=30

sqlite = Setting()
sqlite.drivername='sqlite'
sqlite.database='database.db'
sqlite.data_loader=iterdict_data_loader
sqlite.use_pool=False

mssql = Setting()
mssql.drivername='mssql'
mssql.hostname='localhost'
mssql.username='sa'
mssql.password='StrongPassword123!'
mssql.database='master'
mssql.port=1433
mssql.timeout=30
mssql.driver='ODBC Driver 18 for SQL Server'
mssql.trust_server_certificate='yes'
mssql.data_loader = iterdict_data_loader
mssql.use_pool=False
mssql.pool_max_connections=1
mssql.pool_max_idle_time=300
mssql.pool_wait_timeout=30

Setting.lock()
