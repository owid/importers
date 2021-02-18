import unittest
from HeadCount_Files_Downloader import *
from pathlib import Path

OUTPUT_DIR = "outputTest/headcountsTest"


class test_HeadCount_Files_Downloader(unittest.TestCase):
    def setUp(self):
        self.downloader = HeadCount_Files_Downloader(0, 2, OUTPUT_DIR, max_workers=20)
        self.downloader.ensure_output_dir_exists()

    def test_all_cents_between_dollars(self):
        self.assertEqual(
            all_cents_between_dollars(0, 2)[:10],
            [0.0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09],
        )

    def test_generate_poverty_lines(self):
        lines = generate_poverty_lines_between(0, 0.1)
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
        self.assertEqual(len(generate_poverty_lines_between(0, 60)), 6001)

    def test_request_headcounts_by_poverty_line(self):
        self.skipTest("temp")
        self.assertIsNot(self.downloader.download_one_headcount_file(1), "")

    def test_output_file_names(self):
        self.assertEqual(
            self.downloader.output_filename(1.54),
            "outputTest/headcountsTest/1.54.csv",
        )

    def MANUAL_test_download_one_headcount_file(self):
        self.skipTest()
        self.downloader.download_one_headcount_file(0.50)

    def MANUAL_test_end_to_end(self):
        self.skipTest()
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        self.downloader.download_headcount_files_by_poverty_line()


if __name__ == "__main__":
    unittest.main()