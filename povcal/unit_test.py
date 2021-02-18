import unittest
from main import generate_poverty_lines


class TestMainMethods(unittest.TestCase):
    def test_generate_poverty_lines(self):
        lines = generate_poverty_lines(0, 0.1, 0.01)
        self.assertListEqual(
            lines, [0.0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1]
        )
        self.assertEqual(len(generate_poverty_lines()), 6001)


if __name__ == "__main__":
    unittest.main()