import unittest
import datetime
import etl.transform as tr
from decimal import Decimal

class TransformTest(unittest.TestCase):

    transformer = tr.Transformer()

    def testConvertValue(self):
        # convert string
        value: str = self.transformer.convertValue('tekst')
        expected: str = "'tekst'"
        self.assertEqual(value, expected, 'converted')

        # convert int
        value: int = self.transformer.convertValue(100)
        expected: int = 100
        self.assertEqual(value, expected, 'converted')

        # convert float
        value: float = self.transformer.convertValue(3.5)
        expected: float = 3.5
        self.assertEqual(value, expected, 'converted')

        # convert date
        value: str = self.transformer.convertValue(datetime.date(2024, 6, 17))
        expected: str = "cast ('20240617' as date)"
        self.assertEqual(value, expected, 'converted')

        # convert decimal.Decimal
        value: float = self.transformer.convertValue(Decimal(10))
        expected: float = 10.0
        self.assertEqual(value, expected, 'converted')

    def testFindPattern(self):
        value = self.transformer.findPattern('word1, word2, word3', r',', 0)
        expected = 5
        self.assertEqual(value, expected)

    def testGetFormattedAddress(self):
        dictPlacesAndExpected = {self.transformer.getFormattedAddress('Warsaw'): 'Warsaw, Poland',
                                 self.transformer.getFormattedAddress('Bangalore'): 'Bengaluru, Karnataka, India',
                                 self.transformer.getFormattedAddress('Balance of Bradford'): 'ZERO_RESULTS',
                                 self.transformer.getFormattedAddress('Hollywood'): 'Hollywood, Los Angeles, CA, USA'}

        for value, expect in dictPlacesAndExpected.items():
            self.assertEqual(value, expect)

    def testGetCountry(self):
        dictPlacesAndExpected = {self.transformer.getCountryName('Warsaw'): ' Poland',
                                 self.transformer.getCountryName('Bangalore'): ' India',
                                 self.transformer.getCountryName('Balance of Bradford'): None,
                                 self.transformer.getCountryName('Hollywood'): ' USA'}

        for value, expect in dictPlacesAndExpected.items():
            self.assertEqual(value, expect)

    def testColumnListToString(self):
        value = self.transformer.columnListToString(['a', 'b', 'c', 'd'])
        expected = 'a, b, c, d'
        self.assertEqual(value, expected)

    def testGetListFromString(self):
        formattedAddreess = {'Warsaw': self.transformer.getFormattedAddress('Warsaw'),
                  'Bangalore': self.transformer.getFormattedAddress('Bangalore'),
                  'Balance of Bradford': self.transformer.getFormattedAddress('Balance of Bradford'),
                  'Hollywood': self.transformer.getFormattedAddress('Hollywood')}

        dictAdressesAndExpected = \
        {
          tuple(self.transformer.getListFromString(formattedAddreess['Warsaw'])): tuple(['Warsaw', ' Poland']),
          tuple(self.transformer.getListFromString(formattedAddreess['Bangalore'])): tuple(['Bengaluru', ' Karnataka', ' India']),
          tuple(self.transformer.getListFromString(formattedAddreess['Balance of Bradford'])): tuple(['ZERO_RESULTS']),
          tuple(self.transformer.getListFromString(formattedAddreess['Hollywood'])): tuple(['Hollywood', ' Los Angeles', ' CA', ' USA'])
        }

        for value, expect in dictAdressesAndExpected.items():
            self.assertEqual(value, expect)

        return dictAdressesAndExpected

    def testGetFormattedAddressAsList(self):
        placesEndExpected = {'Warsaw': tuple(['Warsaw', None, 'Poland']),
                             'Bangalore': tuple(['Bengaluru', 'Karnataka', 'India']),
                             'Balance of Bradford': tuple(['Balance of Bradford', None, None]),
                             'Hollywood': tuple(['Los Angeles', 'CA', 'USA'])}

        for place, address in placesEndExpected.items():
            self.assertEqual(tuple(self.transformer.getFormattedAddressAsList(place)), address)


class TestDBConnect(unittest.TestCase):

    conn = tr.DBConnect('oracle', 'target_db', '../connector/authDB.ini')
    def testGetOwner(self):
        value: str = self.conn.getOwner()
        expected: str = 'DWHAMAZONKP'
        self.assertEqual(value, expected)

if __name__ == '__main__':
    unittest.main()