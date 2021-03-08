### Test Files

The test files in the `/test_files` folder should provide some insight into what files data
is required and what it does. 

#### test_cb_account_export.csv
Account export provides two pieces of data, tracking conversions from USD to
USDC and transfers of assets (ETH and USDC currently). Each row represents a
case that is handled or ignored in tests. 
1. Row 2 and 3 (headers on line 1) are a conversion from USD to USDC
2. Row 4 and 5 are a conversion from USDC to USD
3. Row 6 represents a withdrawal in ETH, asset withdraws are needed to remove the 
   basis for that asset from the queue, so it is not considered for future
   fills.
4. row 7 and 8 are USD deposits and withdraws, these are ignored as they are not
   taxable.
5. row 9, 10, and 11 are a trade event, two matches and a fee, this data is
   sourced from the fills export from coinbase.