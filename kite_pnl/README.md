Hi Folks,
This repo will help all those folks, who, like me,  when look for their PnL @Zerodha's Kite trading platform, do not see corporate action adjusted return.
I get corporate action data from Moneycontrol.com (<3) and from bse's own website and then use it calculate the pnl.

## Steps to follow
- Clone the repo
- Place the excel you download from kite, this excel is basically the tradebook.
- Outputs a csv file with name result_pnl_output.csv

## Example
I have added a modified tradebook(removed certain records and orderID column), i also added split and bonus data parsed from moneycontrol when i last ran it.
- Vakrangee is one such stock which had a split in last year and was a part of my portfolio.

## Further Improvements
- Develop a GUI as an inteface, no knowledge of python code requried,might shift it to a webapp
- Better file handling can be done, did not add it because initially I planned it to use it for my own use, but now I feel, it can help a larger community.

Please feel free to modiy the code and use it to your use.
