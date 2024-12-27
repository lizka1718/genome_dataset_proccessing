from pyspark.sql import SparkSession
from pyspark import SparkContext
import time
import logging
import re
import time


#OBJECTIVE I


def find_repeats(sequence):
    return re.findall(r'(.)\1+', sequence)

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    

    spark = SparkSession.builder \
        .appName("FindRepeats") \
        .getOrCreate()
    data = spark.read.text("s3a://retail-employees/data/final_genome.fasta").rdd

    repeats = (data.flatMap(lambda row: find_repeats(row.value))  
                     .map(lambda x: (x, 1))
                     .reduceByKey(lambda a, b: a + b, numPartitions=8)) 

    result = repeats.take(100) 

    logging.info(f"RESULT: {result}")
    for r in result:
        print(r)
    spark.stop()


#OBJECTIVE II



def non_repeating_sequence(sequence: str) -> bool:
    for i in range(len(sequence) - 1):
        if sequence[i] == sequence[i + 1]:
            return True
    return False  

if __name__ == "__main__":
    """
        Non-repeating sequence finder
    """
    APP = "NonRepeatingSequence"
    logging.basicConfig(level=logging.INFO)
    start_time = time.time()
    
    sc = SparkContext(appName=APP)
    spark = SparkSession(sc)
    

    labs = spark.read.text("s3a://retail-employees/data/final_genome.fasta").rdd
    
    filter_sequence = labs.filter(lambda x: not non_repeating_sequence(x[0])) \
                          .map(lambda x: (x[0], 1))


    result = filter_sequence.collect()
    logging.info(f"RESULT: {result}")
    
    for r in result:
        print(r)

    end_time = time.time()
    execution_time = end_time - start_time
    logging.info(f"time: {execution_time} seconds")

    spark.stop()
