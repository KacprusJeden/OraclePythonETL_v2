import unittest
import connector.authorize as auth
from configparser import NoSectionError

class TestBuildConnectionString(unittest.TestCase):


    def testCorrectFile(self):
        actual = auth.getConnectionString('oracle', 'test_db', '../connectorUI/test.ini')
        expected = 'TEST/password@localhost:1521/xepdb1'
        self.assertEqual(actual, expected, 'connection string connect')

    def testIncorrectSession(self):
        with self.assertRaises(NoSectionError, msg='Wrong section'):
            auth.getConnectionString('oracle', 'wrong', '../connector/authDB.ini')


if __name__ == '__main__':
    unittest.main()
