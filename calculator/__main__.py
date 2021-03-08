import argparse
from calculator.tax_calculator import calculate_profit_and_loss


def main():
  args = parse_command_line()
  calculate_profit_and_loss(args.path, args.basis, args.fills, args.track_wash)


def parse_command_line():
  parser = argparse.ArgumentParser()
  parser.add_argument("path", help="Path to files")
  parser.add_argument("basis", help="Name of basis csv in path")
  parser.add_argument("fills", help="Name of fills csv in path")
  parser.add_argument(
    "--track-wash", help="Add to track wash trades", action="store_true")
  return parser.parse_args()


if __name__ == "__main__":
  main()
