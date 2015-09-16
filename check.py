#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Check the status of links within Voyager bib records.
Fill in check.cfg, then run the script like this:

python check.py

or 

python check.py -f bibs_list.txt

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
import glob
import os
import requests
import shutil
import sqlite3 as lite
import sys
import time

today = time.strftime('%Y%m%d') # for csv filename
justnow = time.strftime("%Y-%m-%d %I:%M:%S %p") # for log

# parse configuration file check.cfg
config = ConfigParser.RawConfigParser()
config.read('./config/check.cfg')
indir = config.get('env', 'indir')
outdir = config.get('env', 'outdir')
logdir = config.get('env', 'logdir')
share = config.get('env','share')
db = config.get('db','sqlite')

USER = config.get('vger', 'user')
PASS = config.get('vger', 'pw')
PORT = config.get('vger', 'port')
SID = config.get('vger', 'sid')
HOST = config.get('vger', 'ip')

#===============================================================================
# defs
#===============================================================================
def main(picklist):
	print('-' * 25)
	make_report(picklist)
	if copy_report == True:
		mv_outfiles()
	make_pie()
	print('all done!')


def get_bibs(picklist):
	"""
	If no pick-list is given, query Voyager for the full list and generate a pick-list of all bibs
	"""	
	with open(logdir+'lastbib.txt','rb+') as biblist:
		lastbib = biblist.read()
		
	if lastbib is None or lastbib == '\n':
		lastbib = '0'

	query = """SELECT DISTINCT RECORD_ID 
			FROM ELINK_INDEX 
			WHERE RECORD_TYPE = 'B'
			AND LINK NOT LIKE '%%hdl.handle.net/2027%%'
			AND LINK NOT LIKE '%%datapages.com%%'
			AND RECORD_ID > %s
			AND ROWNUM <= %s
			ORDER BY record_id""" % (lastbib, numorecs)

	dsn = cx_Oracle.makedsn(HOST,PORT,SID)
	oradb = cx_Oracle.connect(USER,PASS,dsn)
		
	rows = oradb.cursor()
	rows.execute(query)
	r = rows.fetchall()
	rows.close()
	oradb.close()
	
	with open(indir+picklist,'wb+') as outfile:
		writer = csv.writer(outfile)
		header = ['BIB_ID']
		writer.writerow(header) 
		for row in r:
			writer.writerow(row)
		
		with open(logdir+'lastbib.txt','wb+') as lastbib, open(logdir+'bib.log','ab+') as biblog:
			lastbib.write(str(row[0]))
			biblog.write(justnow + '\t' + str(row[0])+'\n')


def make_report(picklist):
	"""
	Input is the csv picklist. Output is report with HTTP statuses added.
	"""
	try:
		os.remove(outdir+picklist) # remove output from previous runs
	except OSError:
		pass
		
	with open(indir+picklist,'rb+') as csvfile:
		reader = csv.reader(csvfile, delimiter=',', quotechar='"')
		#firstline = reader.next() # skip header row
		
		with open(outdir+picklist,'ab+') as outfile:
				writer = csv.writer(outfile)
				row = ['bib','title','url','status','redirect','redirect_status','last_checked'] # the header row
				writer.writerow(row) 
		for row in reader:
			if row[0].isdigit():
				bibid = row[0]
				query_elink_index(bibid) # <= check against ELINK_INDEX


def query_elink_index(bibid):
	"""
	Query the ELINK_INDEX table
	"""	
	thisbib = [] # temp list of URLs per bib id; the same URL can appear in a bib more than once; let's check just once
	
	cached = False
	con = lite.connect('./db/cache.db')
	response = ''
	redirect_url = ''
	redirect_status = ''

	dsn = cx_Oracle.makedsn(HOST,PORT,SID)
	db = cx_Oracle.connect(USER,PASS,dsn)
	
	sql = """SELECT RECORD_ID, TITLE_BRIEF, LINK
	FROM
	ELINK_INDEX
	LEFT JOIN BIB_TEXT ON ELINK_INDEX.RECORD_ID = BIB_TEXT.BIB_ID
	WHERE
	RECORD_TYPE='B'
	AND LINK_SUBTYPE like '%%HTTP%%'
	AND RECORD_ID = '%s'"""

	c = db.cursor()
	c.execute(sql % bibid)
	
	for row in c:
		bib = row[0]
		ti = row[1]
		url = row[2]
	
		if url not in thisbib: # if url not already checked just now, under the same bib id...
			if ignore_cache==False: # if indeed checking the cache...
				
				with con:
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
					
				# if the URL's in the cache, status was 200, and it was already checked today, just copy values from cache	
				if cached == True and (response == 200 or redirect_status == 200) and check_date == todaydb:
					resp = response
					redir = redirect_url
					redirst = redirect_status
					last_checked = check_date
				else:
					resp,redir,redirst = get_reponse(url) # ping
					last_checked = time.strftime('%Y-%m-%d %H:%M:%S')
			else: # if ignoring cache (for some good reason?)
				resp,redir,redirst = get_reponse(url)
				last_checked = time.strftime('%Y-%m-%d %H:%M:%S')
				
			if 'requests.exceptions.MissingSchema' in str(resp): # TODO: use try except
				resp = 'bad url'
				
			# put the link and responses in the cache (even if not reading cache on this run)
			with con:
				cur = con.cursor() 
				if cached == False:
					# insert new url into db
					newurl = (bib, url, resp, redir, redirst, last_checked)
					cur.executemany("INSERT INTO bibs VALUES(?, ?, ?, ?, ?,?)", (newurl,))
				else:
					# or, if it was in the cache from a previous run (just in case)
					updateurl = (resp, redir, redirst, last_checked, bib, url)
					cur.executemany("UPDATE bibs SET status=?, redirect=?, redirect_status=?, last_checked=? WHERE bib=? and url=?", (updateurl,))
			
			thisbib.append(url)
						
			newrow = [bib,ti,url,resp,redir,redirst,last_checked]
			
			if verbose:
				print("%s, %s, %s, %s, %s, %s" % (bib,url,resp,redir,redirst,last_checked))
			
			with open(outdir+picklist,'ab+') as outfile:
				if resp != 200 and redirst != 200: # just report out the problems
					writer = csv.writer(outfile)
					writer.writerow(newrow)
		
	thisbib = []

	if con:
		con.close() # sqlite (should already be closed in 'with' blocks, but just in case)
	c.close() # oracle


def get_reponse(url):
	"""
	Get HTTP response for each link
	"""
	redir = ''
	redirstatus = ''
	msg = ''
	connect_timeout = 7.0
	
	try:
		r = requests.get(url, allow_redirects=True, timeout=(connect_timeout))

		if str(r.status_code).startswith('3') and r.history: # catch redirects
			for resp in r.history:
				redirto = resp.headers['Location']
				try:
					requests.get(redirto).status_code
				except: # in case there's a bad redirect URL
					redirstatus = 'bad redirect URL'
				hist = resp.status_code, redirto, redirstatus
			msg = hist
		else:
			msg = r.status_code, redir, redirstatus
			
		return msg
	except requests.exceptions.Timeout as e:
		msg = 'timeout','',''
	except requests.exceptions.HTTPError as e:
		msg = 'HTTPError','',''
	except requests.exceptions.ConnectionError as e:
		msg = 'Connection error','',''
	except requests.exceptions.TooManyRedirects as e:
		msg = 'Too many redirects','',''
	except requests.exceptions.InvalidSchema as e:
		msg = 'Invalid schema','',''
	except:
		msg = sys.exc_info()[0],'',''
	return msg


def mv_outfiles():
	"""
	Move outfiles to network share
	"""
	dest = share
	newname = os.path.splitext(picklist)[0]
	newname += '.csv' # be sure out file is .csv
	
	if not glob.glob(r''+outdir+'*.csv'):
		print("no files to mv?")
		exit

	for f in glob.glob(r''+outdir+picklist):
		try:
			shutil.copyfile(f,dest+newname)
			print("copied %s to %s" % (f, dest+newname))
		except:
			print("problem with moving files: %s" % sys.exc_info()[1])
			pass
  
  
def make_pie():
	"""
	Generate simple pie chart
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
<p>Start date: 09/01/2015. Last report: """+time.strftime('%m/%d/%Y')+""".</p>
<p>Statuses of the <span style='font-size:1.25em'>"""+total+"""</span> URLs checked so far...</p>
<sub><a href='http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html' target="_BLANK">status codes</a></sub>
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
    .color(" ")
    .type("treemap")
    .id("name")
    .size("value")
    .draw()

</script> 
"""
	htmlfile.write(header)
	
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
   
   
if __name__ == "__main__": 
	parser = argparse.ArgumentParser(description='Check ELINK_INDEX tables against list of BIB_IDs')
	parser.add_argument('-f','--filename',required=False,dest="picklist",help="Optional. The name of picklist file, e.g. 'bibs_20150415.csv' (assumed to be in ./in). Can just be a list of BIB_IDs.")
	parser.add_argument("-C", "--ignore-cache",required=False, dest="ignore_cache", action="store_true", help="Optionally ignore the cache to test all URLs freshly.")
	parser.add_argument("-c", "--no-copy",required=False, default=True, dest="copy_report", action="store_false", help="Do not copy the resulting report to the share specified in cfg file.")
	parser.add_argument("-v", "--verbose",required=False, default=False, dest="verbose", action="store_true", help="Print out bibs and urls as it runs.")
	parser.add_argument("-n", "--number",required=False, default=1000, dest="numorecs", help="Number of records to search")
	args = vars(parser.parse_args())
	picklist = args['picklist'] # the list of bibs
	ignore_cache = args['ignore_cache']
	copy_report = args['copy_report']
	verbose = args['verbose']
	numorecs = args['numorecs']
	
	if not picklist: # if no file given...
		picklist = 'links_to_check_'+today+'.csv'
		get_bibs(picklist) # generate a picklist, starting from the bib id in ./log/lastbib.txt

	main(picklist)
	
