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
  min_x = None
  max_x = None
  jet = plt.get_cmap('jet') 

  for i, (kv_str, counts) in enumerate(kv_counts.iteritems()):
    x, y = zip(*counts)
    if(max_x == None):
      max_x = min(x)
      min_x = max(x)
    plt.plot(x, y, color=jet((float(i)/len(kv_str))), linewidth=2.0)


  plt.xlim(min_x, max_x)
  plt.show()


if __name__ == "__main__":
  folder_name = sys.argv[1]
  kv_counts = extractKVCountsFromFolder(folder_name)    
  print(kv_counts)
  graph(kv_counts)
  
