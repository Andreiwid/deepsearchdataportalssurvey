#identify product/platform by API signature
import datetime
import urllib.request
import ssl
import json
import pandas as pd
import sys

#verfies number of arguments
print(str(len(sys.argv)))
print(str(sys.argv))

if len(sys.argv) < 3:
	print("Number of arguments must be 3")
	sys.exit()

#ID to unique identity this running
resultsid = "dataportals_results"

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

urls = pd.read_csv(r'urls.csv', delimiter='|', encoding='latin-1' )

totalurls = len(urls) #get total numer of URLs in the input file
print("totalurls: " + str(totalurls))
totalofsegments = int(sys.argv[1]) #get 1st argument - total of segment
print("totalofsegments: " + str(totalofsegments))
currentsegment = int(sys.argv[2]) #get 2st argument - currentsegment
print("currentsegment: " + str(currentsegment))
currentsegment_start = int((totalurls / totalofsegments) * (currentsegment - 1) + 1)
print("currentsegment_start: " + str(currentsegment_start))
currentsegment_end = int((totalurls / totalofsegments) * currentsegment)
print("currentsegment_end: " + str(currentsegment_end))
segmentsinthisrunning = (currentsegment_end - currentsegment_start) + 1
print("segments_in_this_running: " + str(segmentsinthisrunning))

nonidentified_output_file = open('dataportals_nonidentified-seg' + str(currentsegment) + '-s' + str(currentsegment_start) + '-e' + str(currentsegment_end) + '.txt', 'a+', encoding='latin-1')

lineposrunning = 1 #line position of this running depending on number of segments

linepos = 1 # line position of URLs.csv - this number is different from lineposrunning

for index, row in urls.iterrows():
	if not (linepos >= currentsegment_start and linepos <= currentsegment_end):
		print ('Skipping line ' + str(linepos))
		linepos += 1
		continue #skip line for next for

	completed =  (lineposrunning / segmentsinthisrunning) * 100
	print(str(lineposrunning) + '/' + str(segmentsinthisrunning) + ' (' + str(round(completed)) + '%) ' + 'Reading line ' + str(linepos) + '...')
		
	#configure products signature
	ckan_sig = '/api/3'
	socrata_sig = '/api/catalog/v1'
	arcgis_sig = '/api/v2'
	odsoft_sig = '/api/v2'
	catalog = 'none'

	ckan_error = False
	socrata_error = False
	arcgis_error = False
	odsoft_error = False

	#get redirected domain to verify duplicated URLs
	
	url_id = str(row['ID'])
	url = str(row['URL'])

	try:
		req = urllib.request.Request(
			url, 
			data=None, 
			headers={
				'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
			}
		)
		resp = urllib.request.urlopen(req, timeout=20, context=ctx)
	except:
		print(url_id + '|' + url + '||ERROR')
		nonidentified_output_file.write(str(datetime.datetime.now()) + '|' + url_id + '|' + url + '||ERROR\n')
	else:
		domain = resp.geturl()

		#CKAN
		try:
			req = urllib.request.Request(
				url + ckan_sig, 
				data=None, 
				headers={
					'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
				}
			)
			resp = urllib.request.urlopen(req, timeout=20, context=ctx)
		except Exception as e: #raises HTTP, URL and refused connection errors
			ckan_error = True
		else:
			try:
				response_dict = json.loads(resp.read())
			#except ValueError: #raises JSON format error of a non-expected result (API not found)
			except Exception as e: #raises JSON format error of a non-expected result (API not found)
				pass
			else:
				try:
					if 'version' in response_dict:
						catalog = 'CKAN'
				except TypeError as te: #when URLs respond with a JSON but type is not as expected
					pass

		#SOCRATA
		try:
			req = urllib.request.Request(
				url + socrata_sig, 
				data=None, 
				headers={
					'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
				}
			)
			resp = urllib.request.urlopen(req, timeout=20, context=ctx)
		except: #raises HTTP, URL and refused connection errors
			socrata_error = True
		else:
			try:
				response_dict = json.loads(resp.read())
			#except ValueError: #raises JSON format error of a non-expected result (API not found)
			except Exception as e: #raises JSON format error of a non-expected result (API not found)
				pass
			else:
				try:
					if 'results' in response_dict:
						catalog = 'SOCRATA'
				except TypeError as te: #when URLs respond with a JSON but type is not as expected
					pass

		#ARCGIS
		try:
			req = urllib.request.Request(
				url + arcgis_sig, 
				data=None, 
				headers={
					'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
				}
			)
			resp = urllib.request.urlopen(req, timeout=20, context=ctx)
		except: #raises HTTP, URL and refused connection errors
			arcgis_error = True
		else:
			try:
				response_dict = json.loads(resp.read())
			#except ValueError: #raises JSON format error of a non-expected result (API not found)
			except Exception as e: #raises JSON format error of a non-expected result (API not found)
				pass
			else:
				try:
					if 'datasets' in response_dict and domain.find('hub.arcgis.com') == -1:
						catalog = 'ARCGIS'
				except TypeError as te: #when URLs respond with a JSON but type is not as expected
					pass

		#OPENDATASOFT
		try:
			req = urllib.request.Request(
				url + odsoft_sig, 
				data=None,  
				headers={
					'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
				}
			)
			resp = urllib.request.urlopen(req, timeout=20, context=ctx)
		except: #raises HTTP, URL and refused connection errors
			odsoft_error = True
		else:
			try:
				response_dict = json.loads(resp.read())
			#except ValueError: #raises JSON format error of a non-expected result (API not found)
			except Exception as e: #raises JSON format error of a non-expected result (API not found)
				pass
			else:
				try:
					if 'links' in response_dict:
						catalog = 'OPENDATASOFT'
				except TypeError as te: #when URLs respond with a JSON but type is not as expected
					pass

		if ckan_error and socrata_error and arcgis_error and odsoft_error:
			print(url_id + '|' + url + '|' + domain + '|NONE')
			nonidentified_output_file.write(str(datetime.datetime.now()) + '|' + url_id + '|' + url + '|' + domain + '|none\n')
		elif catalog == 'none':
			print(url_id + '|' + url + '|' + domain + '|NONE')
			nonidentified_output_file.write(str(datetime.datetime.now()) + '|' + url_id + '|' + url + '|' + domain + '|none\n')
		else:
			print(url_id + '|' + url + '|' + domain + '|' + catalog)
			identified_output_file = open('dataportals_identified-seg' + str(currentsegment) + '-s' + str(currentsegment_start) + '-e' + str(currentsegment_end) + '.txt', 'a+', encoding='latin-1')
			identified_output_file.write(str(datetime.datetime.now()) + '|' + url_id + '|' + url + '|' + domain + '|' + catalog + '\n')
			identified_output_file.close()

	lineposrunning += 1
	
	linepos += 1

nonidentified_output_file.close()