from os import listdir
from os.path import isfile
import sys
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm
from collections import defaultdict

def extractKVCountsFromFile(file_name, kv_counts):
  pairs = []
  for line in open(file_name):
    line = line.rstrip()
    key_str, count_str = line.split("\t")
    count = int(count_str)
    kv_str, range_str = key_str.split(" ")
    start_range, stop_range = map(int, range_str[1:len(range_str)-2].split("-"))
    kv_counts[kv_str].append( (start_range, count) )

def extractKVCountsFromFolder(folder_name):
  kv_counts = defaultdict(list) 
  print("Looking for k-v pairs in folder: " + folder_name)
  for file_name in listdir(folder_name):
    file_name = folder_name + "/" + file_name
    if(isfile(file_name) and not file_name.startswith("_")):
      extractKVCountsFromFile(file_name, kv_counts)
  return kv_counts

def graph(kv_counts):
  plt.figure()
  x_values = set([]) 
  for kv_str, (x_lst, y_lst) in kv_counts.iteritems():
    for x in x_lst:
      x_values.add(x)
  x_values = list(x_values)
  min_x = min(x_values)
  max_x = max(x_values)
  all
  

  jet = plt.get_cmap('jet')   
  old_points = (x_values, [0 for k in range(len(x_values))])
  for i, (kv_str, (x, y)) in enumerate(kv_counts.iteritems()):
    if(max_x == None):
      max_x = min(x)
      min_x = max(x)
    c = jet((float(i)/len(kv_str)))
    plt.plot(x, y, color=c, linewidth=2.0)
    plt.fill_between(x,y,old_points[1],color=c)


  plt.xlim(min_x, max_x)
  plt.show()


if __name__ == "__main__":
  folder_name = sys.argv[1]
  kv_counts = extractKVCountsFromFolder(folder_name)    
  for kv_str, counts in kv_counts.iteritems():
    x, y = zip(*counts)
    kv_counts[kv_str] = (x ,y)
  graph(kv_counts)
  
