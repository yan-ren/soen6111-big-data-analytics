import math
import os
import sys
import copy
import time
import random
import pyspark
import io
from statistics import mean
from pyspark.rdd import RDD
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import size, abs
from pyspark.sql.types import *
import numpy as np

'''
INTRODUCTION

With this assignment you will get a practical hands-on of frequent 
itemsets and clustering algorithms in Spark. Before starting, you may 
want to review the following definitions and algorithms:
* Frequent itemsets: Market-basket model, association rules, confidence, interest.
* Clustering: kmeans clustering algorithm and its Spark implementation.

DATASET

We will use the dataset at 
https://archive.ics.uci.edu/ml/datasets/Plants, extracted from the USDA 
plant dataset. This dataset lists the plants found in US and Canadian 
states.

The dataset is available in data/plants.data, in CSV format. Every line 
in this file contains a tuple where the first element is the name of a 
plant, and the remaining elements are the states in which the plant is 
found. State abbreviations are in data/stateabbr.txt for your 
information.
'''

'''
HELPER FUNCTIONS

These functions are here to help you. Instructions will tell you when
you should use them. Don't modify them!
'''

all_states = ["ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc", "fl",
              "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md",
              "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm",
              "ny", "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd",
              "tn", "tx", "ut", "vt", "va", "vi", "wa", "wv", "wi", "wy", "al",
              "bc", "mb", "nb", "lb", "nf", "nt", "ns", "nu", "on", "qc", "sk",
              "yt", "dengl", "fraspm"]


def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark


def toCSVLineRDD(rdd):
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row])) \
        .reduce(lambda x, y: '\n'.join([x, y]))
    return a + '\n'


def toCSVLine(data):
    if isinstance(data, RDD):
        if data.count() > 0:
            return toCSVLineRDD(data)
        else:
            return ""
    elif isinstance(data, DataFrame):
        if data.count() > 0:
            return toCSVLineRDD(data.rdd)
        else:
            return ""
    return None


'''
PART 1: FREQUENT ITEMSETS

Here we will seek to identify association rules between states to 
associate them based on the plants that they contain. For instance, 
"[A, B] => C" will mean that "plants found in states A and B are likely 
to be found in state C". We adopt a market-basket model where the 
baskets are the plants and the items are the states. This example 
intentionally uses the market-basket model outside of its traditional 
scope to show how frequent itemset mining can be used in a variety of 
contexts.
'''


def data_frame(filename, n):
    """
    Write a function that returns a CSV string representing the first
    <n> rows of a DataFrame with the following columns,
    ordered by increasing values of <id>:
    1. <id>: the id of the basket in the data file, i.e., its line number - 1 (ids start at 0).
    2. <plant>: the name of the plant associated to basket.
    3. <items>: the items (states) in the basket, ordered as in the data file.

    Return value: a CSV string. Using function toCSVLine on the right
                  DataFrame should return the correct answer.
    Test file: tests/test_data_frame.py
    """
    spark = init_spark()
    rdd = spark.sparkContext.textFile(filename).take(n)
    rdd = spark.sparkContext.parallelize(rdd) \
        .map(lambda line: line.split(",")) \
        .zipWithIndex() \
        .map(lambda pair: (pair[1], pair[0][0], list(pair[0][1:])))

    return toCSVLine(rdd)


def frequent_itemsets(filename, n, s, c):
    """
    Using the FP-Growth algorithm from the ML library (see
    http://spark.apache.org/docs/latest/ml-frequent-pattern-mining.html),
    write a function that returns the first <n> frequent itemsets
    obtained using min support <s> and min confidence <c> (parameters
    of the FP-Growth model), sorted by (1) descending itemset size, and
    (2) descending frequency. The FP-Growth model should be applied to
    the DataFrame computed in the previous task.

    Return value: a CSV string. As before, using toCSVLine may help.
    Test: tests/test_frequent_items.py
    """
    spark = init_spark()
    rdd = spark.sparkContext.textFile(filename)
    rdd = rdd.map(lambda line: line.split(",")) \
        .zipWithIndex() \
        .map(lambda pair: (pair[1], pair[0][0], list(pair[0][1:])))
    df = spark.createDataFrame(rdd, ["id", "plants", "items"])

    # Create FPGrowth model
    fp_growth = FPGrowth(minSupport=s, minConfidence=c, itemsCol="items")
    model = fp_growth.fit(df)
    freq_itemsets = model.freqItemsets

    # sort frequent itemsets by descending itemset size, then descending frequency
    freq_itemsets = freq_itemsets.sort([size(freq_itemsets.items), "freq"], ascending=[False, False]).limit(n)
    return toCSVLine(freq_itemsets)


def association_rules(filename, n, s, c):
    """
    Using the same FP-Growth algorithm, write a script that returns the
    first <n> association rules obtained using min support <s> and min
    confidence <c> (parameters of the FP-Growth model), sorted by (1)
    descending antecedent size in association rule, and (2) descending
    confidence.

    Return value: a CSV string.
    Test: tests/test_association_rules.py
    """
    spark = init_spark()
    rdd = spark.sparkContext.textFile(filename)
    rdd = rdd.map(lambda line: line.split(",")) \
        .zipWithIndex() \
        .map(lambda pair: (pair[1], pair[0][0], list(pair[0][1:])))
    df = spark.createDataFrame(rdd, ["id", "plants", "items"])

    # Create FPGrowth model
    fp_growth = FPGrowth(minSupport=s, minConfidence=c, itemsCol="items")
    model = fp_growth.fit(df)
    rules = model.associationRules.drop("lift").drop("support")

    rules = rules \
        .sort([size(rules.antecedent), "confidence"], ascending=[False, False]) \
        .limit(n)

    return toCSVLine(rules)


def interests(filename, n, s, c):
    '''
    Using the same FP-Growth algorithm, write a script that computes 
    the interest of association rules (interest = |confidence - 
    frequency(consequent)|; note the absolute value)  obtained using 
    min support <s> and min confidence <c> (parameters of the FP-Growth 
    model), and prints the first <n> rules sorted by (1) descending 
    antecedent size in association rule, and (2) descending interest.

    Return value: a CSV string.
    Test: tests/test_interests.py
    '''
    spark = init_spark()
    # Convert CSV string to DataFrame
    rdd = spark.sparkContext.textFile(filename)
    rdd = rdd.map(lambda line: line.split(",")) \
        .zipWithIndex() \
        .map(lambda pair: (pair[1], pair[0][0], list(pair[0][1:])))
    df = spark.createDataFrame(rdd, ["id", "plants", "items"])

    # Create FPGrowth model
    fp_growth = FPGrowth(minSupport=s, minConfidence=c, itemsCol="items")
    model = fp_growth.fit(df)
    rules = model.associationRules.drop("lift").drop("support")
    freq = model.freqItemsets

    join_sets = rules.join(freq, rules.consequent == freq.items)
    total = df.count()
    join_sets = join_sets.withColumn("interest", abs(join_sets["confidence"] - join_sets["freq"] / total))

    join_sets = join_sets \
        .sort([size(join_sets.antecedent), "interest"], ascending=[False, False]) \
        .limit(n)

    return toCSVLine(join_sets)


'''
PART 2: CLUSTERING

We will now cluster the states based on the plants that they contain.
We will reimplement and use the kmeans algorithm. States will be 
represented by a vector of binary components (0/1) of dimension D, 
where D is the number of plants in the data file. Coordinate i in a 
state vector will be 1 if and only if the ith plant in the dataset was 
found in the state (plants are ordered alphabetically, as in the 
dataset). For simplicity, we will initialize the kmeans algorithm 
randomly.

An example of clustering result can be visualized in states.png in this 
repository. This image was obtained with R's 'maps' package (Canadian 
provinces, Alaska and Hawaii couldn't be represented and a different 
seed than used in the tests was used). The classes seem to make sense 
from a geographical point of view!
'''


def data_preparation(filename, plant, state):
    """
    This function creates an RDD in which every element is a tuple with
    the state as first element and a dictionary representing a vector
    of plant as a second element:
    (name of the state, {dictionary})

    The dictionary should contain the plant names as keys. The
    corresponding values should be 1 if the plant occurs in the state
    represented by the tuple.

    You are strongly encouraged to use the RDD created here in the
    remainder of the assignment.

    Return value: True if the plant occurs in the state and False otherwise.
    Test: tests/test_data_preparation.py
    """
    spark = init_spark()
    rdd = spark.read.text(filename).rdd
    rdd = rdd.map(lambda x: (x.value.split(',')[0], x.value.split(',')[1:])) \
        .flatMap(lambda x: [(state, x[0]) for state in x[1]]) \
        .groupByKey() \
        .map(lambda x: (x[0], {plant: 1 for plant in set(x[1])}))

    result = rdd.filter(lambda x: x[0] == state) \
        .map(lambda x: plant in x[1].keys()) \
        .collect()

    return result[0]


def distance2(filename, state1, state2):
    """
    This function computes the squared Euclidean
    distance between two states.

    Return value: an integer.
    Test: tests/test_distance.py
    """
    spark = init_spark()
    rdd = spark.read.text(filename).rdd
    rdd = rdd.map(lambda x: (x.value.split(',')[0], x.value.split(',')[1:])) \
        .flatMap(lambda x: [(state, x[0]) for state in x[1]]) \
        .groupByKey() \
        .map(lambda x: (x[0], {plant: 1 for plant in set(x[1])}))

    state_rdds = rdd.filter(lambda x: x[0] == state1 or x[0] == state2).collect()

    state1_dict = state_rdds[0][1] if state_rdds[0][0] == state1 else state_rdds[1][1]
    state2_dict = state_rdds[0][1] if state_rdds[0][0] == state2 else state_rdds[1][1]

    keys = set(state1_dict.keys()) | set(state2_dict.keys())
    values1 = np.array([state1_dict.get(k, 0) for k in keys])
    values2 = np.array([state2_dict.get(k, 0) for k in keys])

    return np.sum((values1 - values2) ** 2)


def init_centroids(k, seed):
    """
    This function randomly picks <k> states from the array in answers/all_states.py (you
    may import or copy this array to your code) using the random seed passed as
    argument and Python's 'random.sample' function.

    In the remainder, the centroids of the kmeans algorithm must be
    initialized using the method implemented here, perhaps using a line
    such as: `centroids = rdd.filter(lambda x: x[0] in
    init_states).collect()`, where 'rdd' is the RDD created in the data
    preparation task.

    Note that if your array of states has all the states, but not in the same
    order as the array in 'answers/all_states.py' you may fail the test case or
    have issues in the next questions.

    Return value: a list of <k> states.
    Test: tests/test_init_centroids.py
    """
    random.seed(seed)
    centroids = random.sample(all_states, k)
    return centroids


def first_iter(filename, k, seed):
    """
    This function assigns each state to its 'closest' class, where 'closest'
    means 'the class with the centroid closest to the tested state
    according to the distance defined in the distance function task'. Centroids
    must be initialized as in the previous task.

    Return value: a dictionary with <k> entries:
    - The key is a centroid.
    - The value is a list of states that are the closest to the centroid. The list should be alphabetically sorted.

    Test: tests/test_first_iter.py
    """
    def squared_euclidean_distance(state1_dict, state2_dict):
        keys = set(state1_dict.keys()) | set(state2_dict.keys())
        values1 = np.array([state1_dict.get(k, 0) for k in keys])
        values2 = np.array([state2_dict.get(k, 0) for k in keys])

        return np.sum((values1 - values2) ** 2)

    spark = init_spark()
    centroids = init_centroids(k, seed)
    rdd = spark.read.text(filename).rdd
    rdd = rdd.map(lambda x: (x.value.split(',')[0], x.value.split(',')[1:])) \
        .flatMap(lambda x: [(state, x[0]) for state in x[1]]).filter(lambda x: x[0] in all_states) \
        .groupByKey() \
        .map(lambda x: (x[0], {plant: 1 for plant in set(x[1])}))

    centroids_dict = rdd.filter(lambda x: x[0] in centroids).collect()
    states = rdd.map(
        lambda x: (x[0], {centroid[0]: squared_euclidean_distance(centroid[1], x[1]) for centroid in centroids_dict})). \
        map(lambda x: (min(x[1], key=x[1].get), x[0])). \
        groupByKey().mapValues(lambda x: sorted(list(x))).collectAsMap()

    return {key: states[key] for key in centroids}


def kmeans(filename, k, seed):
    """
    This function:
    1. Initializes <k> centroids.
    2. Assign states to these centroids as in the previous task.
    3. Updates the centroids based on the assignments in 2.
    4. Goes to step 2 if the assignments have not changed since the previous iteration.
    5. Returns the <k> classes.

    Note: You should use the list of states provided in all_states.py to ensure that the same initialization is made.

    Return value: a list of lists where each sub-list contains all states (alphabetically sorted) of one class.
                  Example: [["qc", "on"], ["az", "ca"]] has two
                  classes: the first one contains the states "qc" and
                  "on", and the second one contains the states "az"
                  and "ca".
    Test file: tests/test_kmeans.py
    """
    def squared_euclidean_distance(vec1, vec2):
        return np.sum((np.array(vec1) - np.array(vec2)) ** 2)

    spark = init_spark()
    rdd = spark.read.text(filename).rdd
    all_plants = rdd.map(lambda x: x.value.split(',')[0]).distinct().collect()

    # (state_name, {plant_a: 1, plant_b: 1, ...})
    rdd = rdd.map(lambda x: (x.value.split(',')[0], x.value.split(',')[1:])) \
        .flatMap(lambda x: [(state, x[0]) for state in x[1]]) \
        .filter(lambda x: x[0] in all_states) \
        .groupByKey() \
        .map(lambda x: (x[0], {plant: 1 for plant in set(x[1])}))
    # build 0-1 vector for plant dictionary, 0 means plant not found in state, 1 means plant fount in state
    rdd = rdd.map(lambda x: (x[0], [1 if plant in x[1].keys() else 0 for plant in all_plants]))

    current_iteration = first_iter(filename, k, seed)
    while True:
        next_centroids = []
        i = 0
        for value in current_iteration.values():
            plant_vectors = rdd.filter(lambda x: x[0] in value) \
                .map(lambda x: x[1]) \
                .collect()
            next_centroids.append((i, np.mean(plant_vectors, axis=0)))
            i += 1

        next_iteration = rdd.map(lambda x: (
            x[0], {centroid[0]: squared_euclidean_distance(centroid[1], x[1]) for centroid in next_centroids})). \
            map(lambda x: (min(x[1], key=x[1].get), x[0])). \
            groupByKey().mapValues(lambda x: sorted(list(x))).collectAsMap()

        next_iteration = {centroid[0]: next_iteration[centroid[0]] for centroid in next_centroids}

        if next_iteration == current_iteration:
            break
        else:
            current_iteration = next_iteration

    return list(current_iteration.values())
