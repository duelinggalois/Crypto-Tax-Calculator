## Tax Calculator

I created a tool to calculate profit and loss using the start of year basis valuation for all trading accounts and all trades for that year. The script will match each sell trade to a buy trade, use the minimum size for each find the profit and loss and enter it into a profit and loss table. Once all sells have been processed the remaining buys will be considered the basis for the end of the year. Both a profit and loss and basis csv will result from running the script.
    
Wash trades, recorded losses that are purchased back in a period of less than 30 days, are tracked and accounted for in the profit and loss table. Each basis entry that results in a wash trade will also record the adjusted basis for the unaccounted for loss.

All trades between products with a BTC quote currency (ie ETH-BTC) will result in an api get request to https://api.pro.coinbase.com/ in order to determine the closing price of BTC-USD for the minute of the trade in order to determine a valuation in USD. The closing price may not match the price that Coinbase reports to the IRS in the 1099-k form you may have received, my personal experience was a difference in proceeds of 1/4 of a percent.

Currently the form of th csv's used as input must match coinbase pro's export format. Default headers can be changed in the `format.py` file (maybe in the future I will make this more generic to handle multiple formats)

You should know, I am not an accountant or a tax expert. I claim in no way that the results of this program are accurate. You should consult a tax expert after use to ensure the results are accurate. If you do find inaccuracies after using this program, please let me know so I can attempt to correct them. 

Currently FIFO is the only supported method to support buys. Changing this should not be to difficult by searching the code for FIFO, in which I believe all of the crucial points to change are commented. 

**Requirements**
* [Install Python3](http://docs.python-guide.org/en/latest/starting/install3)
* [Install PipEnv](https://docs.pipenv.org/)

**Install**
* `pipenv install` depending on how you set up pipenv you may need to run as root

**Use**
* have a folder containing a csv file for both the current tax years trades and a csv file with the trades that define your start of year account balance basis (in the case of FIFO the last buys of the prior years trades in which the sum of the sizes of each of those buys would be the account balance at the start of the year). Files should both be formatted with headers seen in the `sample_format.csv`
* `pipenv run python`
* `>>> import tax_calculator`
* `>>> tax_calculator.calculate_all("/path/to/folder/", "basis_trade_file.csv", "trade_file.csv")`
