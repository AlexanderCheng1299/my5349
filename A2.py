pip install pyspark

from google.colab import drive
drive.mount('/content/drive')

from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("COMP5349 A2 Data Loading Example") \
    .getOrCreate()

cuad_data = "/content/drive/MyDrive/comp5349/a2_data/CUADv1.json"
cuad_init_df = spark.read.json(cuad_data)

from pyspark.sql.functions import explode
cuad_data_df= cuad_init_df.select((explode("data").alias('data')))

cuad_paragraph_df = cuad_data_df.select(explode("data.paragraphs").alias("paragraph"))

cuad_context_df = cuad_paragraph_df.select("paragraph.context")

cuad_qas_df = cuad_paragraph_df.select(explode("paragraph.qas").alias("qas"))

cuad_ans_df = cuad_qas_df.select("qas.answers")

cuad_context_df.count()

cuad_qas_df.count()

context_rdd = cuad_context_df.rdd

qas_rdd = cuad_qas_df.rdd

ans_rdd = cuad_ans_df.rdd

a1 = ans_rdd.collect()

a1[1]

from typing import Container
def con_split(record,size=4096,step=2048):
  list1 = []
  #record = record.collect()
  for cur in [record]:
    list2 = []
    pot = 0
    while pot in range(len(cur[0])):
      list = []
      if (pot+size) < len(cur[0]):
        for i in range(size):
          #if pot != 0:
            #list.append(cur[0][pot+i-1])
          #else:
          list.append(cur[0][pot+i])
      else:
        for k in range(len(cur[0])-pot):
          list.append(cur[0][pot+k])
        list2.append(list)
        break
      pot+=step
      list2.append(list)
    list1.append(list2)
  return list1

def get_ans(record):
  for raw in [record]:
    list = []
    if len(raw) > 0:
      for cur in raw[0]:
        start = cur[0]
        end = cur[0]+len(cur[1])
        list.append((start,end))
    else:
      list.append([])
    return list

def split_ans(record):
  i = 0
  record1 = record
  list1 = []
  while i < 20910:
    list = []
    for k in range(41):
      list.append(record[i+k])
    list1.append(list)
    i+=41
  return list1

def find_pos(record):
  i = 0
  list2 = []
  for raw in record:
    list = []
    cotr = cont[i][0]
    limit = len(cotr)
    for ptr in raw:
      list1 = []
      for cur in ptr:
        if cur[0]>2048:
          if cur[0] < 4096:
            st = (1,2)
          elif (limit-cur[0])<2048:
            st = (((cur[0]//2048)+1))
          else:
            bb = cur[0]//2048
            st = (bb,bb+1)
        else:
          st = (1)
        if cur[1]>2048:
          if cur[1] < 4096:
            ed = (1,2)
          elif (limit-cur[1])<2048:
            ed = (((cur[1]//2048)+1))
          else:
            bb = cur[1]//2048
            ed = (bb,bb+1)
        else:
          ed = (1)
        list1.append((st,ed))
      list.append(list1)
    list2.append(list)
  return list2


cont = context_rdd.collect()

len(cont[1][0])

ct = context_rdd.map(con_split)

ct.take(1)

ans1 = ans_rdd.map(get_ans).collect()

ans_gp = split_ans(ans1)

ans_gp

seq_ans = find_pos(ans_gp)

seq_ans[2]

ct1 = ct.collect()

print(ct1[0][0][25])

len(ct)
