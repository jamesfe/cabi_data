"""
    cabi data puller
    http://www.capitalbikeshare.com/data/stations/bikeStations.xml

    csv data links
    http://www.capitalbikeshare.com/trip-history-data

    author: jamesfe
"""

from bs4 import BeautifulSoup
from urllib2 import urlopen
from os.path import basename, join, isfile
from os import listdir
from numpy import genfromtxt
import re

DATA_DIR = "/home/jim/Code/cabi_data/downloads/"


def just_numbers(str_input):
    """
        returns just the numbers from a string
    """
    try:
        ret_vals = re.findall("\D*([0-9]*)\D*", str_input)[0]
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

    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.data_list = self.get_data_list()

    def get_unique_stations(self):
        """
            Define unique stations by running through all the data.
        """
        pass

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


if __name__ == '__main__':

    k = CabiAnalyzer(DATA_DIR)
    print k.data_list
