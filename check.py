#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Check the status of links within Voyager bib records.
Fill in check.cfg, then run the script like this (the usual way):

python check.py -v

or (in special cases, using a list)

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
import unicodecsv

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

	query = """SELECT RECORD_ID, LINK 
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
		header = ['BIB_ID','LINK']
		writer.writerow(header) 
		for row in r:
			bib = str(row[0])
			url = row[1]
			writer.writerow(row)
		
		with open(logdir+'lastbib.txt','wb+') as lastbib, open(logdir+'bib.log','ab+') as biblog:
			lastbib.write(bib)
			biblog.write(justnow + '\t' + bib +'\n')


def make_report(picklist):
	"""
	Input is the csv picklist. Output is report with HTTP statuses added.
	"""
	try:
		os.remove(outdir+picklist) # remove output from previous runs on same day
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
				url = row[1]
				query_elink_index(bibid,url) # <= check against ELINK_INDEX


def query_elink_index(bibid,url):
	"""
	Query the ELINK_INDEX table
	"""	
	thisbib = [] # temp list of URLs per bib id; the same URL can appear in a bib more than once; let's check just once

	#if str(url).decode("utf-8").find(u"\u0332") > 0:
	#	url = str(url).decode("utf-8").replace(u"\u0332", "") # 'combining low line' character has appeared in urls(!)
	
	cached = False
	con = lite.connect('./db/cache.db')
	response = ''
	redirect_url = ''
	redirect_status = ''

	dsn = cx_Oracle.makedsn(HOST,PORT,SID)
	db = cx_Oracle.connect(USER,PASS,dsn)
		
	sql = """SELECT RECORD_ID, TITLE_BRIEF
	FROM
	ELINK_INDEX
	LEFT JOIN BIB_TEXT ON ELINK_INDEX.RECORD_ID = BIB_TEXT.BIB_ID
	WHERE
	RECORD_TYPE='B'
	AND RECORD_ID = '%s'
	AND LINK = '%s'"""

	c = db.cursor()
	c.execute(sql % (bibid,url))
	
	for row in c:
		bib = int(row[0])
		ti = row[1]
		url = url.decode('utf-8')

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
					newurl = (bib, url, str(resp), redir, redirst, last_checked)
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
					writer = unicodecsv.writer(outfile, encoding='utf-8')
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
	parser.add_argument("-C", "--ignore-cache",required=False, default=False,dest="ignore_cache", action="store_true", help="Optionally ignore the cache to test all URLs freshly.")
	parser.add_argument("-c", "--copy",required=False, default=False, dest="copy_report", action="store_true", help="Copy the resulting report to the share specified in cfg file.")
	parser.add_argument("-v", "--verbose",required=False, default=False, dest="verbose", action="store_true", help="Print out bibs and urls as it runs.")
	parser.add_argument("-n", "--number",required=False, default=10000, dest="numorecs", help="Number of records to search")
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
	
