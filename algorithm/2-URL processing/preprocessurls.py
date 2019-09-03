import os
import pandas as pd
import hashlib
import shutil

#function to find the Nth ocorrence of a substring
#necessary to find single slash '/' of a URL and not make confusion with double slash '//'
def findnth(string, substring, n):
    parts = string.split(substring, n + 1)
    if len(parts) <= n + 1:
        return -1
    return len(string) - len(parts[-1]) - len(substring)

completed_lines_hash_url0 = set()

output_file = open('urls.csv', 'a+', encoding='latin-1')
	
#get all files in the current directory
files = [f for f in os.listdir('.') if os.path.isfile(f)]

#make a copy of progress file
shutil.copy('urlprogress.csv', 'urlprogressOLD.csv')

urlprogress = pd.read_csv('urlprogress.csv', delimiter='|', encoding='latin-1')

poslineurl = urlprogress.loc[urlprogress['file'] == 'URLID', 'lastlinepos'].item()

#iterates through result files 
for f in files:
	if 'keyword_results' in f:
		poslinefile = 1

		lastlinepos = urlprogress.loc[urlprogress['file'] == f, 'lastlinepos'].item()
		
		with open(f, mode='r', encoding="latin-1") as f_results:
			for line in f_results:			
				if poslinefile > lastlinepos:
					#just to make sure line has an URL
					if line.find('//') == -1:
						continue
					
					url = line[:line.find('|')]				
					
					url0 = ''
					
					url1stsingleslashpos = findnth(url, '/', 2)

					if url1stsingleslashpos != -1:
						url0 = url[0:url1stsingleslashpos]

					hashUrl0 = hashlib.md5(url0.encode('latin-1')).hexdigest()
					if hashUrl0 not in completed_lines_hash_url0:
						#add a not duplicated URL in the output file
						print(url0)
						output_file.write('URL' + str(poslineurl) + '|' + url0 + '\n')
						completed_lines_hash_url0.add(hashUrl0)
						poslineurl += 1

				poslinefile += 1

		urlprogress.loc[urlprogress['file'] == f, 'lastlinepos'] = poslinefile - 1

urlprogress.loc[urlprogress['file'] == 'URLID', 'lastlinepos'] = poslineurl - 1
		
urlprogress.to_csv('urlprogress.csv', sep='|', index=False, encoding='latin-1')
output_file.close()