# elink_checker

We're methodically checking the status of links within our Voyager bib records.

Fill in check.cfg, then run:
 `python check.py`
 
Optionally, read in a file with bib_ids in the first field:
 `python check.py -f mybibs.txt`
 

For more: `python check.py -h`

#### Requires
* [cx_Oracle](http://cx-oracle.sourceforge.net/) ([installation](https://gist.github.com/kimus/10012910) is a bit involved)

#### Note
Theoretically, link checking could be done with Catjobs 9 & 10 ("HTTP Verification jobs"), though we haven't used them (initial testing was discouraging). Within the Voyager documentation, see Technical.pdf and Reporter.pdf (2-16, 2-23).

