from unittest.mock import patch, MagicMock
import pandas as pd
from main import loadFromProcedure, loadDimCustomerKP, loadDimProductKP, loadDimLocationKP
import unittest

class TestETL(unittest.TestCase):

    @patch('pandas.read_sql_query')
    @patch('pandas.DataFrame.to_sql')
    def testLoadDimCustomerKP(self, mockToSql, mockReadSqlQuery):
        # Mock connections
        mockSrcDB = MagicMock()
        mockTrgDB = MagicMock()

        # Prepare sample data
        dfMock = pd.DataFrame({'cid': [1, 2], 'name': ['Alice', 'Bob']})
        mockReadSqlQuery.return_value = dfMock

        # Call the function
        loadDimCustomerKP(mockSrcDB, mockTrgDB)

        # Check if data was read and saved correctly
        mockReadSqlQuery.assert_called_once_with('SELECT CID, NAME FROM CUSTOMERMASTERKP', mockSrcDB.engine)
        mockToSql.assert_called_once_with('dimcustomerkp', mockTrgDB.engine, if_exists='append', index=False)
        print('Customer KP test passed')

    @patch('pandas.read_sql_query')
    @patch('pandas.DataFrame.to_sql')
    def testLoadDimProductKP(self, mockToSql, mockReadSqlQuery):
        # Mock connections
        mockSrcDB = MagicMock()
        mockTrgDB = MagicMock()

        # Prepare sample data
        dfProductMock = pd.DataFrame({'categoryid': [1, 2], 'pid': [101, 102], 'name': ['ProdA', 'ProdB'], 'price': [10.5, 12.0]})
        dfCategoryMock = pd.DataFrame({'categoryid': [1, 2], 'categoryname': ['CatA', 'CatB']})

        mockReadSqlQuery.side_effect = [dfProductMock, dfCategoryMock]

        # Call the function
        loadDimProductKP(mockSrcDB, mockTrgDB)

        # Check if data was read and joined correctly
        mockReadSqlQuery.assert_any_call('SELECT * FROM PRODUCTMASTERKP', mockSrcDB.engine)
        mockReadSqlQuery.assert_any_call('SELECT * FROM CATEGORYMASTERKP', mockSrcDB.engine)
        mockToSql.assert_called_once_with('dimproductkp', mockTrgDB.engine, if_exists='append', index=False)
        print('Product KP test passed')

    @patch('pandas.read_sql_query')
    @patch('pandas.DataFrame.to_sql')
    def test_loadDimLocationKP(self, mockToSql, mockReadSqlQuery):
        # Mock connections
        mockSrcDB = MagicMock()
        mockTrgDB = MagicMock()

        # Prepare sample data
        df_mock = pd.DataFrame({'city': ['Warsaw', 'Bangalore']})
        mockReadSqlQuery.return_value = df_mock

        # Mock method `getFormattedAddressAsList`
        mockSrcDB.transformer.getFormattedAddressAsList.side_effect = [
            ['Warsaw', None, 'Poland'],
            ['Bangalore', 'Karnataka', 'India']
        ]

        # Call the function
        loadDimLocationKP(mockSrcDB, mockTrgDB)

        # Verify if data was read, processed, and saved correctly
        mockReadSqlQuery.assert_called_once_with('select null as locationkey, null as country, null as state, city from customermasterkp group by city', mockSrcDB.engine)
        mockToSql.assert_called_once_with('dimlocationkp', mockTrgDB.engine, if_exists='append', index=False)
        print('Location KP test passed')

    def testloadFromGENERATEDIMTIME(self):
        mockTrgDB = MagicMock()
        mockTrgDB.executeProcedure = MagicMock()

        # 1: correct
        loadFromProcedure(mockTrgDB, 'GENERATEDIMTIME', 'DIMTIMEKP')
        mockTrgDB.executeProcedure.assert_called_once_with('GENERATEDIMTIME')
        print('Procedure test passed for success case')

        # 2: not exists
        mockTrgDB.executeProcedure.side_effect = Exception('Table or procedure on this schema not exists')
        try:
            loadFromProcedure(mockTrgDB, 'NON_EXISTING_PROCEDURE', 'DIMTIMEKP')
        except Exception as e:
            self.assertEqual(str(e), 'Table or procedure on this schema not exists')
            print('Procedure test passed for exception case')

    def testLoadFromLOAD_FACTSALESKP(self):
        mockTrgDB = MagicMock()
        mockTrgDB.executeProcedure = MagicMock()

        # 1: correct
        loadFromProcedure(mockTrgDB, 'LOAD_FACTSALESKP', 'FACTSALESKP')
        mockTrgDB.executeProcedure.assert_called_once_with('LOAD_FACTSALESKP')
        print('Procedure test passed for success case')

        # 2: not exists
        mockTrgDB.executeProcedure.side_effect = Exception('Table or procedure on this schema not exists')
        try:
            loadFromProcedure(mockTrgDB, 'NON_EXISTING_PROCEDURE', 'FACTSALESKP')
        except Exception as e:
            self.assertEqual(str(e), 'Table or procedure on this schema not exists')
            print('Procedure test passed for exception case')

if __name__ == '__main__':
    unittest.main()