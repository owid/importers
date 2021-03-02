import unittest
from main import *
from HeadCount_Files_Downloader import *
from random import sample
import io


class test_main(unittest.TestCase):
    def test_mega(self):
        self.skipTest("manual")
        df = pd.read_csv("output/mega.csv", header=0)
        df["implied_median"] = df.Median / (365 / 12)
        df.implied_median = df.implied_median.round(2)
        df["diff"] = df.P50 - df.implied_median
        pdb.set_trace()

    # def test_equals(self):
    #     # downloader = HeadCount_Files_Downloader(
    #     #     poverty_lines=[],
    #     #     output_dir=HEADCOUNTS_DIR,
    #     #     detailed_data_dir="",
    #     #     detailed_poverty_lines="",
    #     #     max_workers=1,
    #     # )
    #     # output = ""
    #     # with open(downloader.headcount_output_filename("0.00"), "r") as file:
    #     #     self.assertEqual(output, file.read())

    def test_random_downloads(self):
        downloader = HeadCount_Files_Downloader(
            poverty_lines=[],
            output_dir=HEADCOUNTS_DIR,
            detailed_data_dir="",
            detailed_poverty_lines="",
            max_workers=1,
        )
        pov_lines = [
            poverty_line_as_string(line)
            for line in sample(generate_poverty_lines_between(0, 400), 100)
        ]
        pov_lines = ["34.42"]
        print(f"sampling {pov_lines}")

        for line in pov_lines:
            print(f"testing {line}")
            output = downloader.request_headcounts_by_poverty_line(line)
            df = csv_to_dataframe(output)
            df = downloader.filter_necessary_data(df)

            df2 = pd.read_csv(downloader.headcount_output_filename(line))
            pdb.set_trace()
            self.assertEqual(df.to_string(), df2.to_string())


if __name__ == "__main__":
    unittest.main()