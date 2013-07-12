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
  for kv_str, (x_lst, y_lst) in kv_counts.iteritems():
    i = 0
    while(True):
      if x_lst[i] > x_values[i]:
        x_lst.insert(x_values[i], i -1)
        y_lst.insert(0, i -1)
      elif:
      if x_lst[i] > x_values[i]:

      i += 1
      if i == len(x_values):
        break


  for kv_str, (x_lst, y_lst) in kv_counts.iteritems():        
    print(kv_str + "," + ",".join(map(str, y_lst)))
  print("X," + ",".join(map(str, x_values)))

if __name__ == "__main__":
  folder_name = sys.argv[1]
  kv_counts = extractKVCountsFromFolder(folder_name)    
  for kv_str, counts in kv_counts.iteritems():
    x, y = zip(*counts)
    x = list(x)
    y = list(y)
    kv_counts[kv_str] = (x ,y)
  graph(kv_counts)
  
