
import unittest
from unittest.mock import MagicMock, patch
from db_tools.submit_handler import get_tables

class TestSubmitHandler(unittest.TestCase):

    @patch('db_tools.submit_handler.text')
    def test_get_tables(self, mock_text):
        # Create a mock connection object
        mock_connection = MagicMock()

        # Mock the result of the execute method
        mock_result = MagicMock()
        mock_result.__iter__.return_value = [('table1',), ('table2',)]
        mock_connection.execute.return_value = mock_result

        # Call the function with the mock connection
        tables = get_tables(mock_connection, 'test_db')

        # Assert that the function returns the expected tables
        self.assertEqual(tables, ['table1', 'table2'])

        # Assert that the execute method was called with the correct SQL
        mock_connection.execute.assert_any_call(mock_text('USE `test_db`;'))
        mock_connection.execute.assert_any_call(mock_text('SHOW TABLES;'))

if __name__ == '__main__':
    unittest.main()
