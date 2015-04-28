#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Check the status of links within Voyager bib records.
1. Query ELINK_INDEX, eliminating all hosts that don't need to be checked
2. Select sub-group of these links to check, by RECORD_ID (i.e. BIB_ID)
3. Be sure to fill in check.cfg, then run the script like this:
 `python check.py -f mybibs.txt`

For more: `python check.py -h`

Requires cx_Oracle.

From 20150415
pmg
"""
import argparse
import ConfigParser
import csv
import cx_Oracle
import glob
import os
import pickle
import requests
import shelve
import shutil
import sys
import time

today = time.strftime('%Y%m%d')

# parse configuration file (check.cfg)
config = ConfigParser.RawConfigParser()
config.read('check.cfg')
indir = config.get('env', 'indir')
outdir = config.get('env', 'outdir')
share = config.get('env','share')
shelf_file = config.get('cache','shelf_file')

#===============================================================================
# Checked class
#===============================================================================
class Checked(object):
	def __init__(self):
		self.value = ""
		"""Link as a string"""
		self.response = ""
		""""HTTP response code (or exception)"""
		self.redirect_url = ""
		""""If redirected, the new URL"""
		self.redirect_status = ""
		""""If redirected, response from the new URL"""
		self.check_date = ""
		"""Today's date and time"""

#===============================================================================
# defs
#===============================================================================
def main():
	print('-' * 25)
	setup()
	make_report(picklist)
	if copy_report == True:
		mv_outfiles()
	print('all done!')
	
def setup():
	"""
	Create ./in and ./out dirs if they don't already exist; check ./in (if exists) for picklist.
	"""
	dirs = [indir,outdir]

	for d in dirs:
		if not os.path.exists(d):
			os.makedirs(d)
			
	if not glob.glob(r''+indir+picklist):
		print("'"+picklist+"' isn't in ./in Please move it there and try again.")
		sys.exit()

def make_report(picklist):
	"""
	Input is the csv picklist. Output is report with HTTP statuses added.
	"""
	try:
		os.remove(outdir+picklist) # remove ourput from previous runs
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
	user = config.get('database', 'user')
	pw = config.get('database', 'pw')
	sid = config.get('database', 'sid')
	ip = config.get('database', 'ip')
	
	thisbib = [] # temp list of URLs per bib id; the same URL can appear in a bib more than once; let's check just once
	shelf = shelve.open(shelf_file, protocol=pickle.HIGHEST_PROTOCOL) # cache
	
	dsn_tns = cx_Oracle.makedsn(ip,1521,sid)
	db = cx_Oracle.connect(user,pw,dsn_tns)
	
	sql = """SELECT RECORD_ID, TITLE_BRIEF, LINK
	FROM
	ELINK_INDEX
	LEFT JOIN BIB_TEXT ON ELINK_INDEX.RECORD_ID = BIB_TEXT.BIB_ID
	WHERE
	RECORD_TYPE='B'
	AND LINK_SUBTYPE like '%HTTP%'
	AND RECORD_ID = '%s'"""

	c = db.cursor()
	c.execute(sql % bibid)
	
	for row in c:
		bib = row[0]
		ti = row[1]
		url = row[2]
		
		link = Checked()
		
		if url not in thisbib: # if not already checked just now, under the same bib id...
			if ignore_cache==False: # if checking the cache...
				if shelf[url] and (shelf[url].response == 200 or shelf[url].redirect_status == 200) and today in shelf[url].check_date:
					# if the URL's in the cache, status was 200, and it was already checked today, just copy values from cache
					resp = shelf[url].response
					redir = shelf[url].redirect_url
					redirst = shelf[url].redirect_status
					last_checked = shelf[url].check_date
				else:
					resp,redir,redirst = get_reponse(url)
					last_checked = time.strftime('%Y%m%d %I:%M:%S')
			else:
				resp,redir,redirst = get_reponse(url)
				last_checked = time.strftime('%Y%m%d %I:%M:%S')
				
			# put the link and responses in the cache (even if not reading cache on this run)
			link.value = url
			link.response = resp
			link.redirect = redir
			link.redirect_status = redirst
			link.check_date = last_checked
			shelf[url] = link

			thisbib.append(url)
						
			newrow = [bib,ti,url,resp,redir,redirst,last_checked]
					
			print("%s, %s, %s, %s, %s, %s, %s" % (bib,ti,url,resp,redir,redirst,last_checked))
			
			with open(outdir+picklist,'ab+') as outfile:
				if resp != 200 and redirst != 200: # just report out the problems
					writer = csv.writer(outfile)
					writer.writerow(newrow)
		
	thisbib = []

	c.close()

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
           
if __name__ == "__main__": 
	parser = argparse.ArgumentParser(description='Check ELINK_INDEX tables against list of BIB_IDs')
	parser.add_argument('-f','--filename',required=True,type=str,dest="picklist",help="The name of picklist file, e.g. 'bibs_20150415.csv' (assumed to be in ./in). Can just be a list of BIB_IDs.")
	parser.add_argument("-C", "--ignore-cache",required=False, dest="ignore_cache", action="store_true", help="Optionally ignore the cache to test all URLs freshly.")
	parser.add_argument("-c", "--copy-report",required=False, default=True, dest="copy_report", action="store_false", help="Do not copy the resulting report to the share specified in cfg file.")
	args = vars(parser.parse_args())
	picklist = args['picklist'] # the list of bibs
	ignore_cache = args['ignore_cache']
	copy_report = args['copy_report']
	
	main()
