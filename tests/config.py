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

Setting.lock()
