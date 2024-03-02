import unittest
from src.exo4.no_udf import windows

class TestCalculateTotalPricePerCategoryPerDayIntegration(unittest.TestCase):

    def test_calculate_total_price_per_category_per_day_integration(self):
        # Given
        data = [
            (2500000, '2020-07-06', 7, 23.0, 'furniture'),
            (2500001, '2015-06-10', 6, 91.0, 'furniture'),
            (2500002, '2020-04-17', 13, 53.0, 'furniture'),
            (2500003, '2021-04-03', 4, 60.0, 'food'),
            (2500004, '2018-02-16', 13, 36.0, 'furniture'),
            (2500005, '2019-07-27', 14, 92.0, 'furniture'),
            (2500006, '2020-07-01', 4, 81.0, 'food'),
            (2500007, '2021-05-06', 11, 79.0, 'furniture')
        ]

        # When
        result = windows(data)

        # Then
        self.assertEqual(result[0][0], '2020-07-06')
        self.assertEqual(result[0][1], 'furniture')
        self.assertEqual(result[0][2], 115.0)

if __name__ == '__main__':
    unittest.main()
