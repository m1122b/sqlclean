
import os
import csv
from sqlalchemy import Table, Column, Integer, String, Float, MetaData
from sqlalchemy import create_engine


engine = create_engine('sqlite:///database.db', echo=True)
meta = MetaData()

print(os.getcwd())


with open('clean_stations.csv', newline= '') as csvfile:
    clean_stations = []
    clean_stations_dict = csv.DictReader(csvfile)
    for row in clean_stations_dict:
        # print(row)
        clean_stations.append(row)

clean_stations_header = list(clean_stations[0].keys())
print(clean_stations_header)
for i in range(3):
    print(clean_stations[i])


with open('clean_measure.csv', newline= '') as csvfile:
    clean_measure = []
    clean_measure_dict = csv.DictReader(csvfile)
    for row in clean_measure_dict:
        #print(row)
        clean_measure.append(row)

clean_measure_header = list(clean_measure[0].keys())
print(clean_measure_header)
for i in range(10):
    print(clean_measure[i])


stations = Table(
    'stations', meta,
    Column('id', Integer, primary_key=True),
    Column('station', String),
    Column('latitude', Float),
    Column('longitude', Float),
    Column('elevation', Float),
    Column('name', String),
    Column('country', String),
    Column('state', String),
    )


measures = Table(
    'measures', meta,
    Column('id', Integer, primary_key=True),
    Column('station', String),
    Column('date', String),
    Column('precip', Float),
    Column('tobs', Integer),
    )

meta.create_all(engine)

ins_stations = stations.insert()
ins_measures = measures.insert()

conn = engine.connect()
result_stations = conn.execute(ins_stations, clean_stations)
result_measures = conn.execute(ins_measures, clean_measure)


print(f'engine.driver : {engine.driver}')
print('/')
print(f'engine.table_names : {engine.table_names()}')
print('/')


s = stations.select().where(stations.c.station == 'USC00519397')
result = conn.execute(s)
for row in result:
    print(row)


s = measures.select().where(measures.c.station == 'USC00519397')
result = conn.execute(s)
#for row in result:
#    print(row)

