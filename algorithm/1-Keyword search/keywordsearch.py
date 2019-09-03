import datetime
import urllib.request
import ssl
import json
import sys
import gzip
import shutil
import os

#verfies number of arguments
print(str(len(sys.argv)))
print(str(sys.argv))

if len(sys.argv) < 3:
    print("\nNumber of arguments must be at least 2")
    sys.exit()
	
#set keywords to find in URLs
keywords=["open data","opendata","ckan","socrata","opendatasoft", "arcgis"]
	
num_lines = 0
with open('warc.paths', 'r') as f_path:
    for line in f_path:
        num_lines += 1
f_path.close()

totalpaths = num_lines #get total number of paths in the "warc.paths" file
print("totalpaths: " + str(totalpaths))
totalofsegments = int(sys.argv[1]) #get 1st argument - total of segments
print("totalofsegments: " + str(totalofsegments))
currentsegment = int(sys.argv[2]) #get 2st argument - currentsegment
print("currentsegment: " + str(currentsegment))
currentsegment_start = int((totalpaths / totalofsegments) * (currentsegment - 1) + 1)
print("currentsegment_start: " + str(currentsegment_start))
currentsegment_end = int((totalpaths / totalofsegments) * currentsegment)
print("currentsegment_end: " + str(currentsegment_end))
segmentsinthisrunning = (currentsegment_end - currentsegment_start) + 1
print("segments_in_this_running: " + str(segmentsinthisrunning))

#verify if user wants to continue from a determined position
#if so, open file in append mode instead of creating
if len(sys.argv) == 4:
	if sys.argv[3] != 'continue':
		print("\n3rd argument is invalid. Type 'continue' to start from the last position")
		exit()

	with open('keyword_progress-seg' + str(currentsegment) + '-s' + str(currentsegment_start) + '-e' + str(currentsegment_end) + '.txt', mode='r', encoding='latin-1') as f_progress:
		for line in f_progress:
			lastlinepospaths = line[line.find('linepospaths') + 12:line.find('|')]
	f_progress.close()

	pospaths_continue = int(lastlinepospaths) + 1

	lineposrunning = (pospaths_continue - currentsegment_start) + 1
else:
	#write head of results file
	output_file = open('keyword_results-seg' + str(currentsegment) + '-s' + str(currentsegment_start) + '-e' + str(currentsegment_end) + '.txt', mode='w', encoding="latin-1")
	output_file.write('URL|WARC_SOURCE|KEYWORD_FOUND_POSITION\n')
	output_file.close()
	
	#write head of progres file
	progress_file = open('keyword_progress-seg' + str(currentsegment) + '-s' + str(currentsegment_start) + '-e' + str(currentsegment_end) + '.txt', mode='w', encoding="latin-1")
	progress_file.write('LINE_POS_PATH|WARC_FILE|WARC_PROC_START|WARC_PROC_END|ELAPSED_DOWNLOAD|ELAPSED_DECOMPRESS|ELAPSED_READING|ELAPSED_TOTAL\n')
	progress_file.close()
	
	pospaths_continue = 0
	lineposrunning = 1 #line position of this running depending on number of segments

linepospaths = 1 #line position of warc.paths

with open('warc.paths', 'r') as f_path:
	for line in f_path:
		print("linepos: " + str(linepospaths))
		
		if pospaths_continue == 0:
			if not (linepospaths >= currentsegment_start and linepospaths <= currentsegment_end):
				linepospaths += 1
				continue #skip line for next for
		else:
			if not (linepospaths >= pospaths_continue and linepospaths <= currentsegment_end):
				linepospaths += 1
				continue #skip line for next for

		#register process start
		WARC_PROC_START = datetime.datetime.now()

		url = "https://commoncrawl.s3.amazonaws.com/" + line
		file_name = line[line.rfind('/')+1:].strip()

		#register download start
		WARC_DOWNLOAD_START = datetime.datetime.now()
		
		print("Downloading... " + url)
		download_sucess = 0
		while download_sucess == 0:
			try:
				with urllib.request.urlopen(url, timeout=60) as response, open(file_name, 'wb') as out_file:
					shutil.copyfileobj(response, out_file)
				out_file.close()

			except Exception as e:
				print("Downloading error! Repeting download... " + url)
				print(str(e))
					
			else:
				download_sucess = 1

		#register download end
		WARC_DOWNLOAD_END = datetime.datetime.now()

		#register decompress start
		WARC_DECOMPRESS_START = datetime.datetime.now()
				
		print("Decompressing... " + url)
		with gzip.open(file_name, 'rb') as f_in, open(file_name + '.txt', 'wb') as f_out:
			shutil.copyfileobj(f_in, f_out)
		f_in.close()
		f_out.close()

		#register decompress end
		WARC_DECOMPRESS_END = datetime.datetime.now()
		
		#start counting WARC file line position
		lineposwarc = 1
		
		hasWARCTargetURI = 0
		
		#store results in a temp file
		TEMP_output_file = open('TEMP_keyword_results-seg' + str(currentsegment) + '-s' + str(currentsegment_start) + '-e' + str(currentsegment_end) + '.txt', mode='w', encoding="latin-1")

		#register reading start
		WARC_READING_START = datetime.datetime.now()
		
		with open(file_name + '.txt', mode='r', encoding="latin-1") as f_warc:
			for linewarc in f_warc:
				
				if lineposwarc % 100000 == 0:
					completed =  (lineposrunning / segmentsinthisrunning) * 100
					print(str(lineposrunning) + '/' + str(segmentsinthisrunning) + ' (' + str(round(completed)) + '%) ' + 'Reading line ' + str(lineposwarc) + '...')
				
				#try to identify the beginning of WARC-Type: response
				if linewarc.find('WARC-Target-URI:') != -1:
					WARCTargetURI = linewarc[17:].strip()
					hasWARCTargetURI = 1

				if hasWARCTargetURI == 1:
					if any(keyword in linewarc for keyword in keywords):
						TEMP_output_file.write(WARCTargetURI + '|' + file_name + '|' + str(lineposwarc) + '\n')

						WARCTargetURI = ''
						hasWARCTargetURI = 0

				lineposwarc += 1
		
		f_warc.close()
		TEMP_output_file.close()

		#register reading end
		WARC_READING_END = datetime.datetime.now()
		
		#register process end
		WARC_PROC_END = datetime.datetime.now()
		
		#get the elpased time of each step: DOWNLOAD, DECOMPRESS, READ, TOTAL
		ELAPSED_DOWNLOAD = WARC_DOWNLOAD_END - WARC_DOWNLOAD_START
		ELAPSED_DECOMPRESS = WARC_DECOMPRESS_END - WARC_DECOMPRESS_START
		ELAPSED_READING = WARC_READING_END - WARC_READING_START
		ELAPSED_TOTAL = WARC_PROC_END - WARC_PROC_START

		#append results from temp file to the output file
		output_file = open('keyword_results-seg' + str(currentsegment) + '-s' + str(currentsegment_start) + '-e' + str(currentsegment_end) + '.txt', mode='a+', encoding="latin-1")
		with open('TEMP_keyword_results-seg' + str(currentsegment) + '-s' + str(currentsegment_start) + '-e' + str(currentsegment_end) + '.txt', mode='r', encoding="latin-1") as f_TEMP_output:
			for lineTEMP in f_TEMP_output:
				output_file.write(lineTEMP)
		f_TEMP_output.close()
		output_file.close()
		if os.path.exists('TEMP_keyword_results-seg' + str(currentsegment) + '-s' + str(currentsegment_start) + '-e' + str(currentsegment_end) + '.txt'):
			os.remove('TEMP_keyword_results-seg' + str(currentsegment) + '-s' + str(currentsegment_start) + '-e' + str(currentsegment_end) + '.txt')
	
		#report progress in a file
		progress_file = open('keyword_progress-seg' + str(currentsegment) + '-s' + str(currentsegment_start) + '-e' + str(currentsegment_end) + '.txt', mode='a+', encoding="latin-1")
		progress_file.write('linepospaths' + str(linepospaths) + '|' + file_name + '|' + str(WARC_PROC_START) + '|' + str(WARC_PROC_END) + '|' + str(ELAPSED_DOWNLOAD) + '|' + str(ELAPSED_DECOMPRESS) + '|' + str(ELAPSED_READING) + '|' + str(ELAPSED_TOTAL) + '\n')
		progress_file.close()

		linepospaths += 1
		
		lineposrunning += 1
						
		if os.path.exists(file_name + '.txt'):
			os.remove(file_name + '.txt')
		if os.path.exists(file_name):
			os.remove(file_name)

f_path.close()