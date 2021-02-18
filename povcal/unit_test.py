import unittest
from main import *


class TestMainMethods(unittest.TestCase):
    def test_util(self):
        self.assertEqual(
            all_cents_between_dollars(0, 2)[:10],
            [0.0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09],
        )

    def test_generate_poverty_lines(self):
        lines = generate_poverty_lines(0, 0.1, 0.01)
        self.assertListEqual(
            lines,
            [
                "0.00",
                "0.01",
                "0.02",
                "0.03",
                "0.04",
                "0.05",
                "0.06",
                "0.07",
                "0.08",
                "0.09",
                "0.10",
            ],
        )
        self.assertEqual(len(generate_poverty_lines()), 6001)

    def test_request_headcounts_by_poverty_line(self):
        self.skipTest("temp")
        self.assertIsNot(request_headcounts_by_poverty_line(1), "")

    def test_output_file_names(self):
        self.assertEqual(output_filename(1.54), "output/data_by_poverty_line/1.54.csv")


if __name__ == "__main__":
    unittest.main()