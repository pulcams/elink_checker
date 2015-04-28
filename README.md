# elink_checker

We're methodically checking the status of links within our Voyager bib records. Here's how (roughly):

1. Query ELINK_INDEX, excluding hosts that don't need to be checked
2. Select a sub-group of these links, by RECORD_ID (i.e. BIB_ID). These go into a text file (e.g. mybibs.txt) as a list of BIB_IDs.
3.  Be sure to fill in check.cfg, then run the script:
 `python check.py -f mybibs.txt`

For more: `python check.py -h`

#### Requires
* [cx_Oracle](http://cx-oracle.sourceforge.net/) ([installation](https://gist.github.com/kimus/10012910) is a bit involved)

#### Note
Theoretically, link checking could be done with Catjobs 9 & 10 ("HTTP Verification jobs"), though we haven't used them (initial testing was discouraging). Within the Voyager documentation, see Technical.pdf and Reporter.pdf (2-16, 2-23).

