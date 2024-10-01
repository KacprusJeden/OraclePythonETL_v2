from decimal import Decimal
from datetime import date
import oracledb
from oracledb import LOB
from googlemaps import client
import re
from connector import authorize as auth
from sqlalchemy import create_engine

class Transformer:
    def __init__(self):
        pass

    def convertValue(self, value):
        if isinstance(value, date):
            v = f'''cast ('{str(value.year)}'''
            if value.month < 10:
                v += '0' + str(value.month)
            else:
                v += str(value.month)

            if value.day < 10:
                v += '0' + str(value.day)
            else:
                v += str(value.day)

            v += f"' as date)"
        elif isinstance(value, str):
            v = f"'{value}'"
        elif isinstance(value, Decimal):
            v = float(value)
        elif isinstance(value, LOB):
            v = str(value)
        else:
            v = value

        return v

    def findPattern(self, word: str, pattern: str, order: int) -> int:
        match = list(re.finditer(pattern, word))

        if match:
            return match[order].start()
        else:
            return None

    def getFormattedAddress(self, place: str) -> str:
        API_KEY = auth.getAPIKey('etl/api_key.ini', 'API_KEY')
        API_CLIENT = client.Client(key=API_KEY)

        address = client.places(query=place, client=API_CLIENT)

        if address['status'].upper() == 'OK':
            address = str(address['results'][0]['formatted_address'])
        else:
            address = address['status'].upper()

        return address

    def getFormattedAddressAsList(self, place: str) -> list:
        address = self.getFormattedAddress(place)

        if address != 'ZERO_RESULTS':
            address = self.getListFromString(address)
            address = [add.strip() for add in address]
            if len(address) == 3:
                pass
            elif len(address) < 3:
                address = [address[0], None] + address[1: ]
            else:
                address = address[len(address) - 3: ]
                '''
                    city = address[len(address) - 3]
                    stage = address[len(address) - 2]
                    country = address[len(address) - 1]
                '''

        else:
            address = [place, None, None]

        return address

    def getCountryName(self, place: str) -> str:
        address = self.getFormattedAddress(place)
        if address != 'ZERO_RESULTS':
            index = self.findPattern(address, r',', -1)

            if index is not None:
                return address[index + 1 :] # return country
            else:
                return None

    def columnListToString(self, columnList: list) -> str:
        string = ''
        for i in range(len(columnList)):
            string += columnList[i]
            if i < (len(columnList)) - 1:
                string += r', '

        return string

    def getListFromString(self, word: str) -> list:
        list = []
        idx = 0
        for i in range(len(word)):
            if word[i] == ',':
                list.append(word[idx: i])
                idx = i + 1

        list.append(word[idx: ])

        return list

class DBConnect:
    def __init__(self, implementation: str, section: str, filename: str):
        self.implementation = implementation
        self.section = section
        self.filename = filename
        self.transformer = Transformer()
        self.connectionString = auth.getConnectionString(self.implementation, self.section, self.filename)
        if self.implementation.upper() == 'ORACLE':
            self.connectionStringEngine = auth.getConnectionStringEngine(self.implementation, self.section,self.filename)
            self.engine = create_engine(self.connectionStringEngine)
            self.connection = oracledb.connect(self.connectionString)
            self.cursor = self.connection.cursor()

    def getOwner(self) -> str:
        index = self.transformer.findPattern(self.connectionString, r'/', 0)
        return self.connectionString[: index]

    def getQueryResults(self, query: str) -> list:
        self.cursor.execute(query)
        return list(self.cursor.fetchall())

    def enableAllConstraints(self) -> None:
        query = f"select 'ALTER TABLE ' || owner || '.' || table_name || ' ENABLE CONSTRAINT ' || constraint_name " \
        f"from all_constraints where owner = '{self.getOwner()}' order by constraint_type"
        queriesEnableList = self.getQueryResults(query=query)

        for q in queriesEnableList:
            self.cursor.execute(q[0])
            self.connection.commit()

        print(f'Enabled all constraints on {self.getOwner()}')

    def disableAllConstraints(self) -> None:
        query = f"select 'ALTER TABLE ' || owner || '.' || table_name || ' DISABLE CONSTRAINT ' || constraint_name " \
               f"from all_constraints where owner = '{self.getOwner()}' order by constraint_type desc"
        queriesDisableList = self.getQueryResults(query=query)

        for q in queriesDisableList:
            self.cursor.execute(q[0])
            self.connection.commit()

        print(f'Disabled all constraints on {self.getOwner()}')

    def truncateTable(self, schema=None, table=not None) -> None:
        if schema is None:
            schema = self.getOwner()
        self.cursor.execute(f'TRUNCATE TABLE {schema}.{table}')
        self.connection.commit()
        print(f'Truncated {schema}.{table}')

    def executeProcedure(self, procName: str) -> None:
        self.cursor.callproc(procName)
        self.connection.commit()
        print(f'Procedure {procName} executed')

    def executeFunction(self, funName: str) -> None:
        self.cursor.callproc(funName)
        self.connection.commit()
        print(f'Function {funName} executed')

    def closeConnection(self) -> None:
        self.cursor.close()
        self.connection.close()
        print(f'Connection to {self.getOwner()} closed')