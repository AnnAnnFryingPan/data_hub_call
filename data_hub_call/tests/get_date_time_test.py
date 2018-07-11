import unittest
from data_hub_call import Data_hub_call
import datetime

class TestGetDateTime(unittest.TestCase):
    """
    Our basic test class
    """

    def test_getDateTime(self):
        """
        The actual test.
        Any method which starts with ``test_`` will considered as a test case.
        """

        data_hub_call = Data_hub_call()
        date_time = data_hub_call.get_date_time("Thu 12 Oct 2017 14:30:05")

        self.assertEqual(10, date_time.month)


if __name__ == '__main__':
    unittest.main()