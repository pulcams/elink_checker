# elink_checker

We're methodically checking the status of links within our Voyager bib records.

Fill in check.cfg, then run, for example:
 `python check.py -v`
 
Optionally, read in a file with bib_ids in the first field:
 `python check.py -f mybibs.txt`
 
For more: `python check.py -h`

To (re)create sqlite db (cache): `CREATE TABLE bibs(bib INT, url TEXT, status TEXT, redirect TEXT, redirect_status TEXT, last_checked DATE, PRIMARY KEY (bib, url));`

#### Logic
Query ELINK_INDEX table for links of unsuppressed BIBs and output a csv report. Using this report, check 4 links per host and report out up to 100 problem links for staff to check. Only check links that haven't already been checked in the last 30 days. Timeout is 30 secs. (Each of these constraints can be adjusted using flags; these are just the defaults.)  

#### Requires
* [cx_Oracle](http://cx-oracle.sourceforge.net/) ([installation](https://gist.github.com/kimus/10012910) is a bit involved)
* [requests](http://docs.python-requests.org/en/latest/user/install/)
* sqlite3 `sudo apt-get install sqlite3 libsqlite3-dev`
* unicodecsv `pip install unicodecsv`

#### Note
Theoretically, link checking could be done with Catjobs 9 & 10 ("HTTP Verification jobs"), though we haven't used them (initial testing was discouraging). Within the Voyager documentation, see Technical.pdf and Reporter.pdf (2-16, 2-23).

