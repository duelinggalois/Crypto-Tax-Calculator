from unittest import TestCase, mock
from unittest.mock import MagicMock, call

import calculator


class TestMain(TestCase):

  @mock.patch("calculator.__main__.calculate_all")
  def test_main(self, mock_calc_all: MagicMock):
    path = "/path/to/files/"
    basis = "basis_file"
    fills = "fills_file"
    stub_arg_parser = StubArgParser(path, basis, fills)
    calculator.__main__.argparse = stub_arg_parser

    calculator.__main__.main()

    self.assertEqual(mock_calc_all.call_args_list, [
      call(path, basis, fills)
    ])
    self.assertEqual(stub_arg_parser.parser.added_calls, [
      ("path", "Path to files"),
      ("basis", "Name of basis csv in path"),
      ("fills", "Name of fills csv in path")
    ])


class StubArgs:

  def __init__(self, path: str, basis: str, fills: str ):
    self.path = path
    self.basis = basis
    self.fills = fills


class StubParser:

  def __init__(self, args: StubArgs):
    self.args = args
    self.added_calls = []

  def parse_args(self):
    return self.args

  def add_argument(self, added, help):
    self.added_calls.append((added, help))


class StubArgParser:

  def __init__(self, path: str, basis: str, fills: str):
    self.parser = StubParser(StubArgs(path, basis, fills))

  def ArgumentParser(self):
    return self.parser
