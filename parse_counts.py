"""
WibiDota - parse_counts. Parses the field=value (start_range=end_range) files 
produced by DotaValuesHistogram. Formats the times, aggregates the values to
smaller intervals, and prints the data is csv form. 
"""

from os import listdir
from os.path import isfile
import sys
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm
from collections import defaultdict
from operator import itemgetter
import datetime

import dota_values

DATE_FORMAT = '%m-%d-%Y'
INTERVAL = 60*60*24 * 7 # One week

def kv_counts_from_file(file_name, kv_counts):
  pairs = []
  for line in open(file_name):
    line = line.rstrip()
    key_str, count_str = line.split("\t")
    count = int(count_str)
    kv_str, range_str = key_str.split(" ")
    key, value = kv_str.split("=")
    key = dota_values.get_game_mode(int(value))
    start_range, stop_range = map(int, range_str[1:len(range_str)-2].split("-"))
    kv_counts[key][start_range / INTERVAL] += count

def kv_counts_from_folder(folder_name):
  kv_counts = defaultdict(lambda : defaultdict(int))
  for file_name in listdir(folder_name):
    file_name = folder_name + "/" + file_name
    if(isfile(file_name) and not file_name.startswith("_")):
      kv_counts_from_file(file_name, kv_counts)

  # Aggregates counts, sort the points
  out = defaultdict(list)
  for key, counts in kv_counts.iteritems():
    for count_range, count in counts.iteritems():
      out[key].append((count_range * INTERVAL, count))
  for points in out.itervalues():
    points.sort(key=itemgetter(0))
  return out

def print_csv(kv_counts):
  x_values = set([]) 
  for kv_str, (x_lst, y_lst) in kv_counts.iteritems():
    for x in x_lst:
      x_values.add(x)

  x_values = list(x_values)
  x_values.sort(reverse=False)
  for kv_str, (x_lst, y_lst) in kv_counts.iteritems():
    i = 0
    while(True):
      if len(x_lst) == i or x_lst[i] != x_values[i]:
        x_lst.insert(i, x_values[i])
        y_lst.insert(i, 0)
      i += 1
      if len(x_lst) == len(x_values):
        break

  for kv_str, (x_lst, y_lst) in kv_counts.iteritems():        
    print(kv_str + "," + ",".join(map(str, y_lst)))
  print("X," + ",".join(map(lambda x : (datetime.datetime.fromtimestamp(x).strftime(DATE_FORMAT)), x_values)))

def fill_in_values(kv_counts):
  x_values = set([]) 
  for kv_str, (x_lst, y_lst) in kv_counts.iteritems():
    for x in x_lst:
      x_values.add(x)

  x_values = list(x_values)
  x_values.sort(reverse=False)
  for kv_str, (x_lst, y_lst) in kv_counts.iteritems():
    i = 0
    while(True):
      if len(x_lst) == i or x_lst[i] != x_values[i]:
        x_lst.insert(i, x_values[i])
        y_lst.insert(i, 0)
      i += 1
      if len(x_lst) == len(x_values):
        break
  return x_values

def graph(kv_counts, x_values):
  y_values = np.zeros((len(kv_counts), len(x_values[10:])))
  for i, (x_lst, y_lst) in enumerate(kv_counts.itervalues()):
    y_values[i] = y_lst[10:]

  fig = plt.figure()
  ax = fig.add_subplot(111)
  ax.stackplot(x_values[10:], y_values)
  plt.show()

if __name__ == "__main__":
  folder_name = sys.argv[1]
  kv_counts = kv_counts_from_folder(folder_name)    
  for kv_str, counts in kv_counts.iteritems():
    counts.sort(key=itemgetter(0))
    x, y = zip(*counts)
    x = list(x)
    y = list(y)    
    kv_counts[kv_str] = (x ,y)
  x_values = fill_in_values(kv_counts)
  graph(kv_counts, x_values)
  
