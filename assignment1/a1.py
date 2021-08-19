from pyspark import SparkContext, SparkConf
import sys

sc = SparkContext.getOrCreate()

# ===== cache files ===== #
m = sc.textFile("/Users/tuhinkhare/Work/IISc/Talentsprint-course/pipieline-testing/movie-lens-dataset/ml-latest-small/movies.csv").cache()
r = sc.textFile("/Users/tuhinkhare/Work/IISc/Talentsprint-course/pipieline-testing/movie-lens-dataset/ml-latest-small/ratings.csv").cache()
t = sc.textFile("/Users/tuhinkhare/Work/IISc/Talentsprint-course/pipieline-testing/movie-lens-dataset/ml-latest-small/tags.csv").cache()

"""

```
#######################################
#	TAGS.CSV
#
# Q1: How many distinct users have rated movies? 
# How many distinct users have tagged movies? 
# How many users have both given ratings and tagged movies?
#
```


"""

rud = r.map(lambda l : l.split(",")[0]).distinct()
ruc = (rud.count()-1)
print('Number of users giving ratings is', ruc) # -1 to skip header row


tud = t.map(lambda l : l.split(",")[0]).distinct()
tuc = (tud.count()-1)
print('Number of users tagging movies is', tuc) # -1 to skip header row


trc = (tud.intersection(rud).count() - 1) # -1 to skip header row
print('Number of users rating and tagging movies is', trc)

"""

```
#
# Q2
# What is the average number of ratings given by users? 
# What is the average value of the ratings given by users?
#
```

"""

rv = r.map(lambda l : l.split(",")[2]).filter(lambda l : l != 'rating')
rvs = rv.reduce(lambda a, b: float(a) + float(b)) 	# sum of ratings
rvc = rv.count()	# ratings count
print('Avg rating value is', rvs/rvc)
temp = rvs/rvc

rc = r.count() - 1	# number of ratings
rud = r.map(lambda l : l.split(",")[0]).distinct()
ruc = (rud.count()-1)	# number of distinct users
print('Avg ratings per user is', rc/ruc)

"""

```
#
# Q3
# What is the median value of the ratings given by users?
#
```

"""

rv = r.map(lambda l : l.split(",")[2]).filter(lambda l : l != 'rating')
rvs = rv.sortBy(lambda l: l).zipWithIndex().map(lambda tuple_value: (tuple_value[1], tuple_value[0]))	# index, value...sorted by value

rv = r.map(lambda l : l.split(",")[2]).filter(lambda l : l != 'rating')
rvc = rv.count()	# ratings count

rm = rvs.lookup(rvc/2)[0]
print('Median rating value is', float(rm))

with open('output.txt', 'a') as f:
    f.write('Number of users giving ratings is ' + str(int(ruc)) + '\n')
    f.write('Number of users tagging movies is ' + str(int(tuc)) + '\n')
    f.write('Number of users rating and tagging movies is ' + str(int(trc)) + '\n')
    f.write('Avg rating value is ' + str(temp) +  '\n')
    f.write('Avg ratings per user is ' + str(rc/ruc) + '\n')
    f.write('Median rating value is ' + str(float(rm)))
