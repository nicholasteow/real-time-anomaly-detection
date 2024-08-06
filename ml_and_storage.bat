@echo off
echo Running zmq_sub_casa_lcc.py...
start python Zmq_subscribers\zmq_sub_casa_lcc.py
echo Running lu_data_mongodb_storage.py
start python lu_data_mongodb_storage.py
