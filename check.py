#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Check the status of links within Voyager bib records.
Fill in check.cfg, then run the script like this (the usual way):

`python check.py -v`

...or use a list of bibs (can be used when re-running over a previous list)... 

`python check.py -f bibs_list.txt`

For more: `python check.py -h`

To recreate the sqlite cache:
CREATE TABLE bibs(bib INT, url TEXT, status TEXT, redirect TEXT, redirect_status TEXT, last_checked DATE, PRIMARY KEY (bib, url));

Requires cx_Oracle

From 20150415
pmg
"""
import argparse
import ConfigParser
import csv
import cx_Oracle
import eventlet
import glob
import logging
import os
import re
import requests
import shutil
import sqlite3 as lite
import sys
import time
import unicodecsv
import urllib
from datetime import date, datetime, timedelta
eventlet.monkey_patch()

# TODO
# query update_date from ELINK_INDEX for reporting?
# summary report

today = time.strftime('%Y%m%d') # for csv filename
todaydb = time.strftime('%Y-%m-%d %H:%M:%S') # date for db
justnow = time.strftime("%Y-%m-%d %I:%M:%S %p") # for log

host_list = []

# parse configuration file check.cfg
config = ConfigParser.RawConfigParser()
config.read('./config/check.cfg')
indir = config.get('env', 'indir')
outdir = config.get('env', 'outdir')
logdir = config.get('env', 'logdir')
share = config.get('env','share')

DB = config.get('db','sqlite')
USER = config.get('vger', 'user')
PASS = config.get('vger', 'pw')
PORT = config.get('vger', 'port')
SID = config.get('vger', 'sid')
HOST = config.get('vger', 'ip')

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',filename=logdir+today+'.log',level=logging.INFO)
# the following two lines disable the default logging of requests.get()
#logging.getLogger("requests").setLevel(logging.WARNING)
#logging.getLogger("urllib3").setLevel(logging.WARNING)

#===============================================================================
# defs
#===============================================================================
def main(picklist):
	print('-' * 50)
	print('starting...')
	logging.info('picklist: '+ picklist)
	logging.info('links to check: %s' % numtocheck)
	logging.info('max problem links: %s' % seerslimit)
	logging.info('max age: %s' % maxage)
	logging.info('sample per host: %s' % sample)
	make_report(picklist)
	split_outfile(picklist)
	if copy_report == True:
		mv_outfiles()
	make_tree()
	print('all done!')
	print('-' * 50)
	logging.info('done')
	logging.info('='*75)
	print('bye')


def get_bibs(picklist):
	"""
	If no pick-list is given, query Voyager for the full list
	"""
	if verbose:
		print('hi. running report...')

	# note: advantage of querying bib+url rather than ELINK_ID is to avoid checking dupes (i.e. mult urls per bib)
	query = """SELECT DISTINCT RECORD_ID, LINK, URL_HOST
			FROM ELINK_INDEX 
			WHERE
			RECORD_TYPE='B'
			AND (LINK NOT LIKE '%%app.knovel.com%%'
			AND LINK NOT LIKE '%%arks.princeton.edu%%'
			AND LINK NOT LIKE '%%assets.cambridge.org%%'
			AND LINK NOT LIKE '%%bvbr.bib-bvb.de%%'
			AND LINK NOT LIKE '%%catalog.hathitrust.org%%'
			AND LINK NOT LIKE '%%catalogue.bnf.fr%%'
			AND LINK NOT LIKE '%%catdir.loc.gov%%'
			AND LINK NOT LIKE '%%congressional.proquest.com%%'
			AND LINK NOT LIKE '%%datapages.com%%'
			AND LINK NOT LIKE '%%deposit.d-nb.de%%'
			AND LINK NOT LIKE '%%digital.lib.cuhk.edu.hk%%'
			AND LINK NOT LIKE '%%d-nb.info%%'
			AND LINK NOT LIKE '%%dss.princeton.edu%%'
			AND LINK NOT LIKE '%%dx.doi.org%%'
			AND LINK NOT LIKE '%%eebo.chadwyck.com%%'
			AND LINK NOT LIKE '%%elibrary.worldbank.org%%'
			AND LINK NOT LIKE '%%find.galegroup.com%%'
			AND LINK NOT LIKE '%%galenet.galegroup.com%%'
			AND LINK NOT LIKE '%%gateway.proquest.com%%'
			AND LINK NOT LIKE '%%getit.princeton.edu%%'
			AND LINK NOT LIKE '%%gisserver.princeton.edu%%'
			AND LINK NOT LIKE '%%hdl.handle.net%%'
			AND LINK NOT LIKE '%%ieeexplore.ieee.org%%'
			AND LINK NOT LIKE '%%ilibri.casalini.it%%'
			AND LINK NOT LIKE '%%knowledge.sagepub.com%%'
			AND LINK NOT LIKE '%%latin.packhum.org%%'
			AND LINK NOT LIKE '%%library.princeton.edu%%'
			AND LINK NOT LIKE '%%lib-terminal.princeton.edu%%'
			AND LINK NOT LIKE '%%libweb.princeton.edu%%'
			AND LINK NOT LIKE '%%libweb2.princeton.edu%%'
			AND LINK NOT LIKE '%%libweb5.princeton.edu%%'
			AND LINK NOT LIKE '%%marc.crcnetbase.com%%'
			AND LINK NOT LIKE '%%name.umdl.umich.edu%%'
			AND LINK NOT LIKE '%%ncco.galegroup.com%%'
			AND LINK NOT LIKE '%%onlinelibrary.wiley.com%%'
			AND LINK NOT LIKE '%%opac.newsbank.com%%'
			AND LINK NOT LIKE '%%pao.chadwyck.com%%'
			AND LINK NOT LIKE '%%princeton.lib.overdrive.com%%'
			AND LINK NOT LIKE '%%princeton.naxosmusiclibrary.com%%'
			AND LINK NOT LIKE '%%proquest.safaribooksonline.com%%'
			AND LINK NOT LIKE '%%purl.access.gpo.gov%%'
			AND LINK NOT LIKE '%%purl.fdlp.gov%%'
			AND LINK NOT LIKE '%%roperweb.ropercenter.uconn.edu%%'
			AND LINK NOT LIKE '%%scitation.aip.org%%'
			AND LINK NOT LIKE '%%search.ebscohost.com%%'
			AND LINK NOT LIKE '%%site.ebrary.com%%'
			AND LINK NOT LIKE '%%static.harpercollins.com%%'
			AND LINK NOT LIKE '%%wws-roxen.princeton.edu%%'
			AND LINK NOT LIKE '%%www.aspresolver.com%%'
			AND LINK NOT LIKE '%%www.british-history.ac.uk%%'
			AND LINK NOT LIKE '%%www.elibrary.imf.org%%'
			AND LINK NOT LIKE '%%www.icpsr.umich.edu%%'
			AND LINK NOT LIKE '%%www.ilibri.casalini.it%%'
			AND LINK NOT LIKE '%%www.jstor.org%%'
			AND LINK NOT LIKE '%%www.loc.gov%%'
			AND LINK NOT LIKE '%%www.nap.com%%'
			AND LINK NOT LIKE '%%www.netread.com%%'
			AND LINK NOT LIKE '%%www.pppl.gov%%'
			AND LINK NOT LIKE '%%www.sciencedirect.com%%'
			AND LINK NOT LIKE '%%www.slavery.amdigital.co.uk%%'
			AND LINK NOT LIKE '%%www.sourceoecd.org%%'
			AND LINK NOT LIKE '%%www.springerlink.com%%'
			AND LINK NOT LIKE '%%www.springerprotocols.com%%'
			AND LINK NOT LIKE '%%www-wds.worldbank.org%%'
			AND LINK NOT LIKE '%%dramonline.org%%'
			AND LINK NOT LIKE '%%blackwellreference.com%%'
			AND LINK NOT LIKE '%%ark.cdlib.org%%')
			AND LINK_SUBTYPE like '%%HTTP%%'
			ORDER BY record_id"""

	dsn = cx_Oracle.makedsn(HOST,PORT,SID)
	oradb = cx_Oracle.connect(USER,PASS,dsn)
		
	rows = oradb.cursor()
	rows.execute(query)
	r = rows.fetchall()
	
	rows.close()
	oradb.close()
	
	with open(indir+picklist,'wb+') as outfile:
		writer = csv.writer(outfile)
		header = ['BIB_ID','LINK','URL_HOST']
		writer.writerow(header) 
		c = 0
		for row in r:
			bib = str(row[0]) # to put in log files below
			url = row[1]
			url = re.sub(ur'[\u0332]','',url.decode('utf8')).encode('utf8') # tentative, based on appearance of this one char.
			host = row[2]
			writer.writerow((bib,url,host))
			c += 1
		
	logging.info('bibs in ELINK_INDEX: %s' % str(c))
	if verbose:
		print('wrote out report')


def make_report(picklist):
	"""
	Input is the csv picklist. Output is report with HTTP statuses added.
	"""	
	try:
		os.rename(outdir+picklist, outdir+picklist + '.bak') # back up output from previous run on same day (once)
	except OSError:
		pass

	with open('./temp/count.txt','wb+') as countfile:
		countfile.write(str('0'))

	with open(indir+picklist,'rb+') as csvfile:
		reader = csv.reader(csvfile, delimiter=',', quotechar='"')
		
		with open(outdir+picklist,'ab+') as outfile, open(logdir+today+'_details.csv','wb+') as reasonsfile:
				writer = csv.writer(outfile)
				row = ['bib','title','host','url','status','redirect','redirect_status','last_check_in_days','suppressed','f040','f945','ldr07'] # the header row of report for SeERs staff
				writer.writerow(row) # <= a file will be generated with a header row even if there were no links to report
				detailsrow = ['bib','host','url','resp','redir','redirst','last_checked','last_check_in_days','cached', 'pinged','count'] # header row for detailed log file
				detailswriter = csv.writer(reasonsfile)
				row.append('check_count')
				detailswriter.writerow(detailsrow)
		try:
			for row in reader:
				if row[0].isdigit(): # first col should be bib_id and this skips header row if there is one
					bibid = row[0]
					url = row[1].decode('utf-8')
					host = row[2]
					q = query_elink_index(bibid,url,host) # <= check against ELINK_INDEX to get more data and check link if appropriate
					if q == 'done':
						return
		except Exception as e:
			exc_type,exc_obj,exc_tb = sys.exc_info()
			logging.info('problem: %s obj: %s line: %s' % (exc_type,exc_obj,exc_tb.tb_lineno))
		with open('./temp/count.txt','rb') as countfile:
			count = countfile.read().rstrip('\n')
			logging.info('pinged: %s' % count)
		with open(outdir+picklist,'rb') as reportfile:
			for i, line in enumerate(reportfile):
				pass
			logging.info('reported (bad): %s' % i)
			
				
def query_elink_index(bibid,url,host):
	"""
	Query the ELINK_INDEX table
	"""
	bib = bibid
	cached = False
	check_date = None
	con = lite.connect(DB)
	datediff = 0
	gov040 = ''
	gov945 = ''
	ldr07 = ''
	last_checked = todaydb
	pinged = 'n'
	redir = ''
	redirst = ''
	resp = ''
	response = ''
	redirect_url = ''
	redirect_status = ''
	suppressed = ''
	ti = ''
	url = url.replace("'","''") # any single quotes need to be doubled in SQL
	
	dsn = cx_Oracle.makedsn(HOST,PORT,SID)
	db = cx_Oracle.connect(USER,PASS,dsn)
		
	sql = """SELECT DISTINCT ELINK_INDEX.RECORD_ID, SUBSTR(BIB_TEXT.TITLE_BRIEF,1,25), BIB_MASTER.SUPPRESS_IN_OPAC, princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '945','a') as f945a, princetondb.GETALLBIBTAG(BIB_MASTER.BIB_ID, '040') as f040,
	SUBSTR(BIB_TEXT.BIB_FORMAT,2,1) as ldr07
	FROM
	ELINK_INDEX
	LEFT JOIN BIB_TEXT ON ELINK_INDEX.RECORD_ID = BIB_TEXT.BIB_ID
	LEFT JOIN BIB_MASTER ON BIB_TEXT.BIB_ID = BIB_MASTER.BIB_ID
	WHERE
	ELINK_INDEX.RECORD_TYPE='B'
	AND ELINK_INDEX.RECORD_ID = '%s'
	AND ELINK_INDEX.LINK = '%s'
	AND BIB_MASTER.SUPPRESS_IN_OPAC <> 'Y'"""

	c = db.cursor()
	c.execute(sql % (bibid,url))
		
	with open('./temp/count.txt','rb+') as countfile:
		count = countfile.read().rstrip('\n')
		if count == '':
			count = 0
		else:
			count = int(count)
	
	if count < int(numtocheck):
		try:
			for row in c:
				bib = row[0]
				ti = row[1]
				suppressed = row[2]
				gov945 = 'govdoc' if re.search('DOCS',str(row[3])) else 'none'
				gov040 = 'govdoc' if re.search('MvI',str(row[4]),re.IGNORECASE) else 'none'
				ldr07 = row[5]
	
				if ignore_cache==False: # if checking the cache...	
					with con:
						url = url.decode('utf-8')
						con.row_factory = lite.Row
						cur = con.cursor()
						cur.execute("SELECT * FROM bibs WHERE bib=? and url=?",(bib,url,))
						rows = cur.fetchall()
						if len(rows) == 0:
							cached = False
						else:
							cached = True
							for row in rows:
								response = row['status']
								redirect_status = row['redirect_status']
								check_date = row['last_checked']
								
					# get number of days since bib was last checked
					if cached == True and check_date is not None:
						date2 = datetime.strptime(todaydb,'%Y-%m-%d %H:%M:%S')
						date1 = datetime.strptime(str(check_date),'%Y-%m-%d %H:%M:%S')
						datediff = abs((date2 - date1).days)
	
					if cached == True and (datediff < maxage): #and response == 200):
						# don't bother to re-check if link was last checked before 'maxage' date
						resp = response # from cache
						redir = redirect_url # from cache
						redirst = redirect_status # from cache
						last_checked = check_date # from cache
					else:
						if host_list.count(host) < int(sample): # <= if haven't checked `sample` recs per this host
							resp,redir,redirst = get_response(url) # <= check link
							host_list.append(host)
							last_checked = time.strftime('%Y-%m-%d %H:%M:%S')
							count += 1
							pinged = 'y'
							
				else: # if ignoring cache
					if host_list.count(host) < int(sample):
						resp,redir,redirst = get_response(url) # <= check link
						host_list.append(host)
						last_checked = time.strftime('%Y-%m-%d %H:%M:%S')
						count += 1
						pinged = 'y'
						
				if last_checked != '' and ignore_cache == False and str(resp) != '': # (if sample has been surpassed, status will be '')
					with con:
						cur = con.cursor() 
						if cached == False:
							# insert new url and statuses into db...
							newurl = (bib, url, str(resp), redir, redirst, last_checked)
							cur.executemany("INSERT INTO bibs VALUES(?, ?, ?, ?, ?, ?)", (newurl,))
						else:
							# or, if it was in the cache from a previous run but was from before the max date...
							updateurl = (last_checked, str(resp), redir, redirst, bib, url)
							cur.executemany("UPDATE bibs SET last_checked=?,status=?,redirect=?,redirect_status=? WHERE bib=? and url=?", (updateurl,))
		
				newrow = [bib, ti, host, url, resp, redir, redirst, datediff, suppressed, gov040, gov945, ldr07] # SeERs report
					
				if verbose:
					print("%s checked -- %s, %s, %s, %s, %s, %s, %s, %s, %s, %s" % (count,bib,host,url,resp,redir,redirst,last_checked,suppressed,cached,pinged))
					
				# this may just be a temporary file to make sure the counts are correct (per host and per run)
				with open(logdir+today+'_details.csv','ab+') as detailsfile:
					details = [bib,host,url,resp,redir,redirst,last_checked,datediff,cached,pinged,count]
					detailswriter = unicodecsv.writer(detailsfile, encoding='utf-8')
					detailswriter.writerow(details)
					
				with open(outdir+picklist,'ab+') as outfile:
					if ((str(resp) != '' and str(resp) != '200' and str(redirst) != '200') and (str(last_checked[:10]) == todaydb[:10]) and (str(resp) != 'SSL Error')): # just report out the fresh problems, and leave out ssl errors
						report_writer = unicodecsv.writer(outfile, encoding='utf-8')
						report_writer.writerow(newrow)
				outlength = check_file_len(outdir+picklist)
				if outlength >= (seerslimit + 1): # +1 for the header row
					logging.info('Quitting after reporting bad links (%s)' % seerslimit)
					return 'done'
		except Exception as e:
			exc_type,exc_obj,exc_tb = sys.exc_info()
			logging.info('problem in : %s obj: %s line: %s' % (exc_type,exc_obj,exc_tb.tb_lineno))
	else:
		return 'done'

	with open('temp/count.txt','wb+') as countfile:
		countfile.write(str(count))


def check_file_len(fname):
	"""
	Check the length of the output file, in case we want to set limits on it (e.g. 100 per week)
	Nicked from GH: http://stackoverflow.com/questions/845058/how-to-get-line-count-cheaply-in-python
	"""
	with open(fname) as f:
		for i, l in enumerate(f):
			pass
	return i + 1

	
def get_response(url):
	"""
	Get HTTP response for each link
	"""
	redir = ''
	redirstatus = ''
	msg = ''
	connect_timeout = 30.0
	url = str(url).strip()
	try:
		with eventlet.Timeout(connect_timeout): # <= this is needed to prevent hanging on large pdfs			
			if 'web.lexis-nexis.com' in url: # this is a bit flakey; trying it out
				r = requests.get(url, allow_redirects=True, headers={'Accept': '*/*','User-Agent': 'python-requests/1.2.0'})
			else:
				r = requests.head(url, allow_redirects=True, headers={'Accept': '*/*','User-Agent': 'python-requests/1.2.0'})

			if r.status_code == 403: # change user-agent
				r = requests.head(url, allow_redirects=True, headers={'Accept': '*/*','User-Agent': 'Mozilla/5.0'})
			elif r.status_code == 405 or r.status_code == 500: # try GET ...
				r = requests.get(url, allow_redirects=True, headers={'Accept': '*/*','User-Agent': 'python-requests/1.2.0'})
				
			if str(r.status_code).startswith('3') and r.history: # catch redirects
				for resp in r.history:
					redirto = resp.headers['Location']
					try:
						requests.head(redirto).status_code
					except: # in case there's a bad redirect URL
						redirstatus = 'bad redirect URL'
					hist = resp.status_code, redirto, redirstatus
				msg = hist
			else:
				msg = r.status_code, redir, redirstatus
			return msg
			
	except eventlet.timeout.Timeout:
		msg = 'timeout','',''
	except requests.exceptions.HTTPError as e:
		msg = 'HTTPError','',''
	except requests.exceptions.TooManyRedirects as e:
		msg = 'Too many redirects','',''
	except requests.exceptions.InvalidSchema as e:
		msg = 'Invalid schema','',''
	except requests.exceptions.InvalidURL as e:
		msg = 'Invalid URL','',''
	except requests.exceptions.MissingSchema as e:
		msg = 'Bad url','',''
	except requests.exceptions.SSLError as e:
		msg = 'SSL Error','',''
	except KeyboardInterrupt as e:
		msg = 'stopped','',''
	except UnicodeEncodeError as e:
		msg = 'unicode error','',''
	except requests.exceptions.ConnectionError as e:
		msg = 'Connection error','',''
	except:
		msg = sys.exc_info()[0],'',''
	return msg


def split_outfile(picklist):
	"""
	Split the output into 3 separate reports: gov docs, serials, and general
	"""
	msg = 'splitting report ('
	punct = ''
	# first just create header rows (there's got to be a better way, but this works for now)
	with open(outdir+'ser_'+picklist,'wb+') as serials,open(outdir+'gov_'+picklist,'wb+') as govdoc,open(outdir+'gen_'+picklist,'wb+') as general:
		row1 = ['bib','title','host','url','status','redirect','redirect_status','last_check_in_days','suppressed','f040','f945','ldr07']
		serwriter = csv.writer(serials)
		serwriter.writerow(row1)
		govwriter = csv.writer(govdoc)
		govwriter.writerow(row1)
		genwriter = csv.writer(general)
		genwriter.writerow(row1) 

	# write data
	with open(outdir+picklist,'rb+') as outfile:
		reader = csv.reader(outfile, delimiter=',', quotechar='"')
		header_row = next(reader)				
		for row in reader:
			f040 = row[9]
			f945 = row[10]
			ldr07 = row[11]
			if ldr07 in ['s','i']:
				with open(outdir+'ser_'+picklist,'ab+') as serials:
					serwriter = csv.writer(serials)
					serwriter.writerow(row)
			elif f945 == 'govdoc' or f040 == 'govdoc':
				with open(outdir+'gov_'+picklist,'ab+') as govdoc:
					govwriter = csv.writer(govdoc)
					govwriter.writerow(row)
			else:
				with open(outdir+'gen_'+picklist,'ab+') as general:
					genwriter = csv.writer(general)
					genwriter.writerow(row) 

	# delete if no links, just header row
	filelist = [outdir+'ser_'+picklist, outdir+'gov_'+picklist, outdir+'gen_'+picklist]
	for f in filelist:
		if os.stat(f).st_size == 98: # this is fragile, st_size of a file with just the header row is 98
			os.remove(f)
		else:
			if msg != 'splitting report (':
				punct = ','
			msg += punct+os.path.basename(f)[:3]
	logging.info(msg + ')')


def mv_outfiles():
	"""
	Move outfiles to network share
	"""
	dest = share
	newname = os.path.splitext(picklist)[0]
	newname += '.csv' # be sure out file is .csv
	
	if not glob.glob(r''+outdir+'*'+today+'.csv'):
		print("no files to mv?")
		exit

	for f in glob.glob(r''+outdir+'*'+picklist):
		try:
			shutil.copyfile(f,dest+newname)
			msg = "copied %s to %s" % (f, dest+newname)
			logging.info(msg)
		except:
			print("problem with moving files: %s" % sys.exc_info()[1])
			pass

  
def make_tree():
	"""
	Generate simple treemap
	"""
	htmlfile = open('./html/elink.html','wb+')
	
	con = lite.connect('./db/cache.db')
	with con:
		con.row_factory = lite.Row
		cur = con.cursor()
		cur.execute("select count(bib) from bibs")
		rows = cur.fetchall()
		for row in rows:
			total = str(row[0])
	
	header = """<!doctype html>
<meta charset="utf-8">
<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css">
<script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/js/bootstrap.min.js"></script>
<script src="http://www.d3plus.org/js/d3.js"></script>
<script src="http://www.d3plus.org/js/d3plus.js"></script>
<div class="container">
<h1>Voyager Link Check</h1>
<a href="https://github.com/pulcams/elink_checker" target="_BLANK">github</a>
<p>Start date: 11/23/2015. Latest report: """+time.strftime('%m/%d/%Y')+""".</p>
<p>Statuses of the <span style='font-size:1.25em'>"""+total+"""</span> URLs checked so far...</p>"""

	body = """<sub><a href='http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html' target="_BLANK">status codes</a></sub>
<div id="viz" style="margin:10px 10px 10px 0px;height:600px;"></div>
</div>
<script>

 /*var attributes = [
    {"name": "200", "hex": "#B2F0B2"}
  ]*/

  var data = ["""

	footer = """]
   d3plus.viz()
    .container("#viz")
    .data(data)
    .color(function(d){
      return d.value > 0 ? "#BDDEBD" : "#ADD6AD";
    })
    .type("treemap")
    .id("name")
    .size("value")
    .draw()

</script> 
"""
	htmlfile.write(header)
	htmlfile.write('<table class="table-condensed table-bordered">\n<tr><td>status</td><td>no. of links</td></tr>\n')
	with con:
		con.row_factory = lite.Row
		cur = con.cursor()
		cur.execute("select status, count(status) from bibs group by status")
		rows = cur.fetchall()
		for row in rows:
			response = str(row[0])
			count = row[1]
			htmlfile.write('<tr><td>%s</td><td>%s</td></tr>\n' % (response,count))
	htmlfile.write("</table>")
	htmlfile.write(body)
	
	rownum = 0
	with con:
		con.row_factory = lite.Row
		cur = con.cursor()
		cur.execute("select status, count(status) from bibs group by status")
		rows = cur.fetchall()
		for row in rows:
			response = str(row[0])
			count = row[1]
			if rownum == 0:
				htmlfile.write('{"value":%s,"name":"%s"}' % (count,response))
			elif rownum > 0:
				htmlfile.write(',\n{"value":%s,"name":"%s"}' % (count,response))
			rownum += 1
	htmlfile.write(footer)
	con.close()
	logging.info('wrote elink.html')


#===============================================================================
# __main__
#===============================================================================
if __name__ == "__main__": 
	parser = argparse.ArgumentParser(description='Check ELINK_INDEX tables against list of BIB_IDs')
	parser.add_argument('-f','--filename',required=False,dest="picklist",help="Optional. The name of picklist file, e.g. 'bibs_20150415.csv' (assumed to be in ./in). Can just be a list of BIB_IDs.")
	parser.add_argument("-C", "--ignore-cache",required=False, default=False,dest="ignore_cache", action="store_true", help="Optionally ignore the cache to test all URLs freshly.")
	parser.add_argument("-c", "--copy",required=False, default=False, dest="copy_report", action="store_true", help="Copy the resulting report to the share specified in cfg file.")
	parser.add_argument("-v", "--verbose",required=False, default=False, dest="verbose", action="store_true", help="Print out bibs and urls as it runs.")
	parser.add_argument("-n", "--number",required=False, default=1500, dest="numtocheck", help="Number of links to check")
	parser.add_argument("-s", "--sample",required=False, default=4, dest="sample", help="Max number of urls per domain")
	parser.add_argument('-a','--age',dest="maxage",help="Max days after which to re-check",required=False, default=90)
	parser.add_argument("-l", "--limit",required=False, default=100, dest="seerslimit", help="Max number of urls for the SeERs unit to check")
	args = vars(parser.parse_args())

	copy_report = args['copy_report']
	ignore_cache = args['ignore_cache']
	maxage = int(args['maxage'])
	numtocheck = args['numtocheck']
	picklist = args['picklist'] # the list of bibs
	sample = args['sample']
	seerslimit = args['seerslimit']
	verbose = args['verbose']

	logging.info('='*75)
	if not picklist: # if no file given, run query against vger...
		picklist = 'links_to_check_'+today+'.csv'
		get_bibs(picklist)

	main(picklist)
	make_tree()
