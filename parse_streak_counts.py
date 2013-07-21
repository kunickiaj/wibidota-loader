import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm
from collections import defaultdict
from os import listdir
from os.path import isfile
import sys

DELIM = ","

TYPES = {"win" : lambda x : x == "true", "score" : int, "interval" : int, "value" : int} 

def values_from_file(file_name):
  values = []
  for line in open(file_name):
    value = {}
    line_key, line_val = line.rstrip().split("\t")
    line_parts = line_key.split(",")
    for part in line_parts:
      part_key, part_value = part.split("=")
      value[part_key] = TYPES[part_key](part_value)
    value['value'] = TYPES['value'](line_val)
    values.append(value)
  return values

def values_from_folder(folder_name):
  values = []
  for file_name in listdir(folder_name):
    file_name = folder_name + "/" + file_name
    if(isfile(file_name) and not file_name.startswith("_")):
      values += values_from_file(file_name)
  return values


if __name__ == "__main__":
  values = values_from_folder(sys.argv[1])
  data_by_interval = defaultdict(lambda : defaultdict(lambda : [0] * 2))
  for value in values:
    place = 0
    if value['win']:
      place = 1
    data_by_interval[value['interval']][value['score']][place] += value['value']
  
  for interval, scores in sorted(data_by_interval.iteritems()):
    print("*" * 10 + " INTERVAL: " + str(interval / 60) + " " + "*" * 10)
    for score, (won, loss) in sorted(scores.iteritems(), key = lambda x : int(x[0])):
      print(str(score) + " " + str((won, loss)))


  lines = []
  labels = []
  for interval, scores in sorted(data_by_interval.iteritems()):
    if(interval != 60 * 15):
      continue
    x = []
    y=  []
    for score, (won, loss)  in sorted(scores.iteritems()):
      if( (won + loss) < 1000) or abs(score) > 5:
        continue
      x.append(score)
#      y.append( won + loss)
      y.append(float(won) / float(won + loss))
    l, = plt.plot(x ,y)
    lines.append(l)
    labels.append("Game Distance = " + str(interval/60) + " minutes")
  plt.ylabel("win rate")
  plt.xlabel("games won/lost")
  plt.legend(lines, labels, fontsize=8)
  plt.show()
