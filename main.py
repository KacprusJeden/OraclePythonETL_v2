# Project ETL in Python
# Copyright Kacper Prusiński 2024

from etl import transform
import pandas as pd
from datetime import datetime

def loadFromProcedure(dbconn: transform.DBConnect, procedure: str, trgTale: str) -> None:
    try:
        print(f'{trgTale} LOADING...\n')
        dbconn.executeProcedure(procedure)
        today = datetime.now()
        print(f'{trgTale} LOADED at {today}\n')
    except:
        print('Table or procedure on this schema does not exist')


def loadDimCustomerKP(srcDB, trgDB):
    print(f'DIMCUSTOMERMASTERKP LOADING...\n')
    df = pd.read_sql_query('SELECT CID, NAME FROM CUSTOMERMASTERKP', srcDB.engine)
    df = df.rename(columns={'cid': 'customerkey', 'name': 'customername'})
    print(df)
    df.to_sql('dimcustomerkp', trgDB.engine, if_exists='append', index=False)
    today = datetime.now()
    print(f'DIMCUSTOMERMASTERKP LOADED at {today}\n')


def loadDimProductKP(srcDB, trgDB):
    print(f'DIMPRODUCTKP LOADING...\n')
    df = pd.read_sql_query('SELECT * FROM PRODUCTMASTERKP', srcDB.engine)
    dfProd = pd.read_sql_query('SELECT * FROM CATEGORYMASTERKP', srcDB.engine)

    df.set_index('categoryid', inplace=True)
    dfProd.set_index('categoryid', inplace=True)

    dfJoined = df.join(dfProd[['categoryname']], how='left')
    dfJoined = dfJoined[['pid', 'name', 'price', 'categoryname']]
    dfJoined = dfJoined.rename(columns={'pid': 'productkey', 'name': 'productname', 'price': 'productprice'})
    print(dfJoined)
    dfJoined.to_sql('dimproductkp', trgDB.engine, if_exists='append', index=False)

    today = datetime.now()
    print(f'DIMPRODUCTKP LOADED at {today}\n')


def loadDimLocationKP(srcDB, trgDB):
    print(f'DIMLOCATIONKP LOADING...\n')
    query = 'select null as locationkey, null as country, null as state, city from customermasterkp group by city'
    df = pd.read_sql_query(query, srcDB.engine)

    for index, row in df.iterrows():
        df.loc[index, 'locationkey'] = index + 1
        location = srcDB.transformer.getFormattedAddressAsList(df.loc[index, 'city'])
        df.loc[index, 'state'] = location[1]
        df.loc[index, 'country'] = location[2]

    print(df)
    df.to_sql('dimlocationkp', trgDB.engine, if_exists='append', index=False)

    today = datetime.now()
    print(f'DIMLOCATIONKP LOADED at {today}\n')



# create ETL objects to connection to OracleDB
srcDB = transform.DBConnect('oracle', 'source_db', 'connector/authDB.ini')
trgDB = transform.DBConnect('oracle', 'target_db', 'connector/authDB.ini')

print(f'Connection to {srcDB.getOwner()} open')
print(f'Connection to {trgDB.getOwner()} open\n')

today = datetime.now()
print(f'ETL Start time: {today}\n')

# prepare tables on target

# this is a list of table to load
trgTableList = {'DIMTIMEKP': "loadFromProcedure(trgDB, 'GENERATEDIMTIME', 'DIMTIMEKP')",
                'DIMCUSTOMERKP': 'loadDimCustomerKP(srcDB, trgDB)',
                'DIMPRODUCTKP': 'loadDimProductKP(srcDB, trgDB)',
                'DIMLOCATIONKP': 'loadDimLocationKP(srcDB, trgDB)',
                'FACTSALESKP': "loadFromProcedure(trgDB, 'LOAD_FACTSALESKP', 'FACTSALESKP')"
                }

# disable constraints on target
trgDB.disableAllConstraints()
print()

# truncate tables before loading or reloading data
for tab in trgTableList.keys():
    trgDB.truncateTable(schema=None, table=tab)

print()

# appropriate ETL
for tab, exec in trgTableList.items():
    eval(exec)

# enable constraints
trgDB.enableAllConstraints()

# close connections
srcDB.closeConnection()
trgDB.closeConnection()

del srcDB, trgDB

today = datetime.now()
print(f'\nETL End time: {today}')
