"""
    cabi data puller
    http://www.capitalbikeshare.com/data/stations/bikeStations.xml

    csv data links
    http://www.capitalbikeshare.com/trip-history-data


    author: jamesfe
"""

from bs4 import BeautifulSoup
from urllib2 import urlopen
from os.path import basename, join
from numpy import genfromtxt
import re


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


def csv_to_manipulations():
    """
        convert csv files into a sqlite database
    """
    d2014_4 = genfromtxt("./downloads/2010-4th-quarter.csv",
                         dtype=None, delimiter=",")
    return d2014_4

def pull_data():
    """
       download data to a local directory.  mostly in zip format.
    """
    in_csv_page = 'http://www.capitalbikeshare.com/trip-history-data'
    base_url = 'http://www.capitalbikeshare.com'
    download_path = "./downloads/"

    in_urlopen = urlopen(in_csv_page).read()
    links = BeautifulSoup(in_urlopen).find_all('a')

    for candidate in links:
        href_link = candidate.get('href')
        if href_link.find("/assets/") > -1:
            tgt_url = base_url+href_link
            print tgt_url
            local_file = open(join(download_path, basename(href_link)), 'w')
            local_file.write(urlopen(tgt_url).read())
            local_file.close()


def count_items():
    """
        there was some weirdness with the headers.  double-check that the
        column counts are the same with this function; but really, 
        make sure that the # sign isn't jacking things up for you.
    """
    in_file = open("./downloads/2010-4th-quarter.csv", 'r')
    num_items = set()
    for line in in_file:
        num_items.add(len(line.split(",")))
    print num_items

if __name__ == '__main__':
    csv_to_manipulations()
    #count_items()
