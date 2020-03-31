import argparse
from calculator.tax_calculator import calculate_all


def parse_command_line():
  parser = argparse.ArgumentParser()
  parser.add_argument("path", help="Path to files")
  parser.add_argument("basis", help="Name of basis csv in path")
  parser.add_argument("fills", help="Name of fills csv in path")
  return parser.parse_args()


def main(args):
  calculate_all(args.path, args.basis, args.fills)


if __name__ == "__main__":
  args = parse_command_line()
  main(args)
