## Tax Calculator

I created a tool to calculate profit and loss using the start of year basis
valuation for all trading accounts and all trades for that year. The script will
match each sell trade to a buy trade, use the minimum size for each find the
profit and loss and enter it into a profit and loss table. Once all sells have
been processed the remaining buys will be considered the basis for the end of
the year. Both a profit and loss and basis csv will result from running the
script.

Currently, the form of the csv's used as input must match coinbase pro's export
format. Default headers can be changed in the `format.py` file (maybe in the
future I will make this more generic to handle multiple formats)

All trades between products with a BTC quote currency (ie ETH-BTC) will result
in an api get request to https://api.pro.coinbase.com/ in order to determine the
closing price of BTC-USD for the minute of the trade in order to determine a
valuation in USD. I have been advised that transferring basis from one asset to 
another is not advisable in these cases, thus the value in USD is required to
determine the proceeds of one and basis of the other. The closing price may not
match the price that Coinbase reports to the IRS in the 1099-k form you may have
received, my personal experience was a difference in proceeds of 1/4 of a
percent. 

In 2021, I am adding support for ETH transfers out of coinbase to the Ethereum
network and tracking gas expenses from transactions on chain. See
[Must Haves 2020](https://github.com/rev3ks/Crypto-Tax-Calculator/projects/3)
for progress. I will aim to do this in a way that other transfers onto other
chains can be easily added in the future. Currently, in design phase. Trying to
figure out a way to require minimal exports or api's like
[The Graph](https://thegraph.com) or [EtherScan](https://etherscan.io). Right
now I am trying to figure out how to best link transactions from coinbase to a
transfer info from on contract. This doesn't seem possible without also
uploading withdraws from coinbase and matching transfers to eth accounts. 

Please note that I am not an accountant, or a tax expert. I claim in no way that
the results of this program are accurate. You should consult a tax expert after
using to ensure the results are accurate. If you do find inaccuracies after
using this program, please let me know and I will attempt to correct them. 

Currently, FIFO is the only supported method to support buys. Changing this
should not be too difficult by searching the code for FIFO, where I believe all
the crucial points to change are noted. 

**Requirements**
* [Install Python3](http://docs.python-guide.org/en/latest/starting/install3)
* [Install PipEnv](https://docs.pipenv.org/)

**Install**
* `pipenv install` depending on how you set up pipenv you may need to run as root

**Use**
* have a folder containing a csv file for both the current tax years trades and 
  a csv file with the trades that define your start of year account balance
  basis (in the case of FIFO the last buys of the prior years trades in which 
  the sum of the sizes of each of those buys would be the account balance at the
  start of the year). Files should both be formatted with headers seen in the
  [sample_format](sample_format.csv).
* For isolating basis for asset withdrwas see the [test csv](test/README.md) 
  for more information on the needed account exports. 
* `$ pipenv run python -m calculator /path/to/folder/ basis_trade_file.csv trade_file.csv`
  Wash loss trading is not tracked by default but can be tracked and losses 
  invalidated and added to basis of the trade that washes the loss by passing
  `--track-wash` to the script.