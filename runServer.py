__author__ = 'weiyushen'

import os
import time
from os import walk
from multiprocessing import Pool

#find crawl.py
path_crawl = '/home/ubuntu/Files/'
os.chdir(path_crawl)
from crawl import *

#Set Total Jobs: Globe Variable
##############################
#CAREFUL!!!
Total_jobs = 5
Set_limits = 3000
#CAREFUL!!!
##############################


#functions
###########################################################################################
#generate time
def gen_time():
    ftime = time.localtime()
    list_time = str(ftime.tm_year) + '_' + str(ftime.tm_mon) + '_' + str(ftime.tm_mday)+ '_' + str(ftime.tm_hour)
    count_time = ftime.tm_hour
    return [list_time, count_time]

def __check(record):
    #print 'Numbers of reviews:',len(record['Reviews']['ReviewList'])
    if len(record['Reviews']['ReviewList']) == 0:
        print 'Warning: Empty File'
        return False
    else:
        return True

#fetch and store data
def start_fetch(itemurl, path, address):

    try:
        print "item:", itemurl
        itemurl = itemurl.strip()
        record = { 'itemurl' : itemurl}

        valid = fetchItem(itemurl, record) # pull the item

        if valid and __check(record):
            h = hashlib.md5()
            h.update(itemurl)
            print h.hexdigest()
            if not os.path.exists(path + '/' +address+'/'):
                os.makedirs(path + '/' +address+'/')

            with open(path + '/' +address+'/' + h.hexdigest() + '.json', 'w') as _file:
                json.dump(record, _file)

    except:
        print 'Error: fail to read', itemurl

def get_url_from_file(source):
    os.chdir(source)
    f = []
    for (dirpath, dirnames, filenames) in walk(source):
        f.extend(filenames)

    f.remove('.DS_Store')

    result = []
    address = []
    for urlItem in f:
        limit = 0
        with open(source + urlItem, 'r') as _file:
            lines = _file.readlines()
            for line in lines:
                if limit < Set_limits:
                    address.append(f)
                    result.append(line)
                    limit += 1
    return result, address

#Fix Kindle data missing issue
def read_json(_filename, path):
    try:
        h = hashlib.md5()
        h.update(_filename)
        _filename_ = path + '/' + h.hexdigest() + '.json'
        with open(_filename_, 'r') as json_file:
            return json.load(json_file)
    except:
            print 'cannot open', _filename

def get_missing_data(json_data):
    _itemurl = json_data["itemurl"]
    print _itemurl

    # maximum three failures
    no_error = True
    tries = 0
    while tries < 3:

        try:
            req = urllib2.Request(_itemurl)

            response = urllib2.urlopen(req)
            #page = response.read()

            page = response.read()
            #print page
            soup = BeautifulSoup(page)
            list = soup.find('b', class_='priceLarge')
            list_price = list.get_text()
            list_price = str(list_price)
            list_price = list_price.strip()
            #print list_price
            json_data["ListPrice"] = list_price

            offer = soup.find('td', class_='listPrice')
            offer_price = offer.get_text()
            offer_price = str(offer_price)
            offer_price = offer_price.strip()
            json_data["OfferPrice"] = offer_price

            name = soup.find('title').get_text()
            name = str(name)
            json_data["Name"] = name

            tries = 3
        except:
            tries += 1
            no_error = False

    if no_error == True:
        return json_data
    else:
        return None

def _save(url, json_data, path):
    h = hashlib.md5()
    h.update(url)
    _filename = path + '/' + h.hexdigest() + '.json'
    with open(_filename, 'w') as _file:
        json.dump(json_data, _file)

def start(thread_number):

    #find urlTxt
    path_urlTxt = '/home/ubuntu/Files/Txturl/'

    url_list, url_address =  get_url_from_file(path_urlTxt)
    #print 'loading in', len(url_list), 'items'

    #Set data storing directory
    path_store = '/home/ubuntu/Files/Results/'
    #create directory
    timestamp = gen_time()
    timestamp = timestamp[0]

    os.chdir(path_store)
    if not os.path.exists(timestamp):
        os.makedirs(timestamp)

    path_store = path_store + timestamp

    #generate sub list
    x = int(len(url_list)*1.0/Total_jobs) + 1
    a = thread_number * x
    b = (thread_number + 1) * x

    sub_list = url_list[a:b]
    sub_address = url_address[a:b]

    #report
    print 'Thread', thread_number, 'is working'
    print 'data from', a, 'to', b, 'is loaded'

    #fetch data here
    for i in range(0,len(sub_list)):

        start_fetch(sub_list[i], path_store,sub_address[i])
'''
        if 'Kindle' in i:
            try:
                input_ = read_json(i, path_store)
                input_ = get_missing_data(input_)
                _save(i, input_, path_store)
            except:
                print 'Fail to get missing data for Kindle'
'''


###########################################################################################

#MultiThreading

while True:
    try:
        jobs = range(0,Total_jobs)
        p = Pool(Total_jobs)
        p.map(start, jobs)
        break
    except:
        print 'Something wrong, try again'










