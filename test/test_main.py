from unittest import TestCase, mock
from unittest.mock import MagicMock, call

import calculator


class TestMain(TestCase):

  @mock.patch("calculator.__main__.calculate_all")
  @mock.patch("calculator.__main__.argparse._sys")
  def test_main(self, mock_sys: MagicMock, mock_calc_all: MagicMock):
    script = "/path/of/running/script/discarded/by/argparse"
    path = "/path/to/files/"
    basis = "basis_file"
    fills = "fills_file"
    mock_sys.argv = [script, path, basis, fills]

    calculator.__main__.main()

    self.assertEqual(mock_calc_all.call_args_list, [
      call(path, basis, fills, False)
    ])

  @mock.patch("calculator.__main__.calculate_all")
  @mock.patch("calculator.__main__.argparse._sys")
  def test_main_with_wash(self, mock_sys: MagicMock, mock_calc_all: MagicMock):
    script = "/path/of/running/script/discarded/by/argparse"
    path = "/path/to/files/"
    basis = "basis_file"
    fills = "fills_file"
    wash_flag = "--track-wash"
    mock_sys.argv = [script, path, basis, fills, wash_flag]

    calculator.__main__.main()

    self.assertEqual(mock_calc_all.call_args_list, [
      call(path, basis, fills, True)
    ])
