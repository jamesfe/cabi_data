"""
    cabi data puller
    http://www.capitalbikeshare.com/data/stations/bikeStations.xml

    csv data links
    http://www.capitalbikeshare.com/trip-history-data

    author: jamesfe
"""

from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
from urllib2 import urlopen
from os.path import basename, join, isfile
from os import listdir
import time
from datetime import datetime
# from numpy import genfromtxt
import re

# DATA_DIR = "/home/jim/Code/cabi_data/downloads/"
DATA_DIR = "/Users/jimmy1/PersCode/cabi_data/testdata2"


def just_numbers(str_input):
    """
        returns just the numbers from a string
    """
    try:
        ret_vals = int(re.findall("\D*([0-9]*)\D*", str_input)[0])
    except IndexError:
        ret_vals = None
    return ret_vals


def timestr_to_sec(time_str):
    """
        convert a time string "0hr 5min 35sec." to a valid number of seconds
    """
    breaks = [just_numbers(_) for _ in time_str.split(" ")]

    tot_seconds = breaks[2] + (breaks[1] * 60) + (breaks[0] * 3600)
    return tot_seconds


def pull_data():
    """
       download data to a local directory.  mostly in zip format.
    """
    in_csv_page = 'http://www.capitalbikeshare.com/trip-history-data'
    base_url = 'http://www.capitalbikeshare.com'

    in_urlopen = urlopen(in_csv_page).read()
    links = BeautifulSoup(in_urlopen).find_all('a')

    for candidate in links:
        href_link = candidate.get('href')
        if href_link.find("/assets/") > -1:
            tgt_url = base_url+href_link
            print tgt_url
            local_file = open(join(DATA_DIR, basename(href_link)), 'w')
            local_file.write(urlopen(tgt_url).read())
            local_file.close()


class CabiAnalyzer:
    """ 
        Class to manage analyzing cabi data 
    """
    #   d2014_4 = genfromtxt(join(DATA_DIR, "2010-4th-quarter.csv"),
    #                         dtype=None, delimiter=",")

    def __init__(self, data_dir, index_name):
        """
        create a connection to ES, make an index that is specified by the
        user, scan through a list of files and begin to add a line from each
        to the database as individual documents
        :param data_dir: directory with csv files in it
        :param index_name: elasticsearch index name
        :return:
        """
        self.data_dir = data_dir
        self.data_list = list()
        self.es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])
        self.index_name = index_name
        # self.clear_elasticsearch(self.index_name)
        # Debating whether or not we should automatically delete all that data.
        self.es.indices.create(index=self.index_name, ignore=400)
        self.process_data()

    # def get_unique_stations(self):
    #     """
    #         Define unique stations by running through all the data.
    #     """
    #     pass

    def process_data(self):
        """
            run a loop against parse file and put each file into ES
        :return:
        """
        self.data_list = self.get_data_list()
        if len(self.data_list) == 0:
            print "No data to read."
        for i in xrange(0, len(self.data_list)):
            self.parse_file(i)

    def parse_file(self, file_index):
        """
        after we've gathered a list of files, we pass an index to this function
        it goes into the list and grabs that file then parses it line by line
        :param file_index: int from 0 to len(self.data_list)
        :return:
        """
        count = 0
        this_file = self.data_list[file_index]
        for line in open(this_file, 'r'):
            if count == 0:
                count += 1
                continue
            in_data = line.strip().split(",")
            seconds = timestr_to_sec(in_data[0])
            fromtime = datetime.strptime(in_data[1], '%m/%d/%Y %H:%M')
            fintime = datetime.strptime(in_data[2], '%m/%d/%Y %H:%M')
            # print in_data
            start_stn_num = re.findall("([0-9]{5})", in_data[3])[0]
            start_stn_addr = in_data[3].split("(")[0].strip()

            fin_stn_num = re.findall("([0-9]{5})", in_data[4])[0]
            fin_stn_addr = in_data[4].split("(")[0].strip()

            bike_id = in_data[5]
            user_type = in_data[6]

            add_data = {"triplength": seconds,
                        "starttime": fromtime,
                        "fintime": fintime,
                        "start_stn_num": start_stn_num,
                        "start_stn_addr": start_stn_addr,
                        "fin_stn_num": fin_stn_num,
                        "fin_stn_addr": fin_stn_addr,
                        "bike_id": bike_id,
                        "user_type": user_type,
                        "from_to_quick": start_stn_num + "_" + fin_stn_num
            }
            self.es.create(self.index_name, "rides", add_data)
            count += 1
        print "Inserted " + str(count) + " objects."

    def get_data_list(self):
        """
            Go to the target directory.  Find and return an array of files
            that we can then analyze.
        """
        ret_vals = list()
        tgt_dir = self.data_dir
        for c_file in listdir(tgt_dir):
            if isfile(join(tgt_dir, c_file)):
                if c_file[-3:].lower() == 'csv':
                    ret_vals.append(join(tgt_dir, c_file))
        return ret_vals

    def clear_elasticsearch(self, index_name):
        """
        prep for a new data entry by clearing the entire elasticsearch index
        :return:
        """
        self.es.indices.delete(index=index_name, ignore=[400, 404])


if __name__ == '__main__':
    new_index_name = "cabi_data_" + str(time.time()).split(".")[0]
    k = CabiAnalyzer(DATA_DIR, new_index_name)
    print k.es
    print k.data_list

    data = {
        "length": 311,
        "dateout": "12/30/2010 22:53",
        "datein": "12/30/2010 22:59",
        "fromstn": "4th St & Massachusetts Ave NW (31604)",
        "tostn": "Columbus Circle / Union Station (31623)",
        "bikeid": "W00592",
        "ridertype": "Registered"
    }


