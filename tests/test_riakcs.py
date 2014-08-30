# import unittest
# from tests.common import get_check, read_data_from_file
#
# class RiakCsTestCase(unittest.TestCase):
#
#     def testMetrics(self):
#         test_data = read_data_from_file('riakcs_in.json')
#         expected = eval(read_data_from_file('riakcs_out.python'))
#         riakcs, instances = get_check('riakcs', self.riakcs_config)
#         parsed = riakcs._parse_stats(test_data)
#         self.assertEquals(parsed.sort(), expected.sort())
