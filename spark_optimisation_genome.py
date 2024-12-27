from pyspark.sql import SparkSession
from pyspark import SparkContext
import time
import logging


#OBJECTIVE I

# def repetition_finder(sequence:str):
#     repeats = []
#     current_char = None
#     count = 0
#     for char in sequence:
#         if char == current_char:
#             count += 1
#         else:
#             if count > 2:
#                 repeats.append((current_char * count, 1))
#                 current_char = char
#                 count = 1
#     if count > 1:
#         repeats.append((current_char * count, 1))
#     return repeats

# if __name__ == "__main__":
#     """
#         repetetion finder
#     """
#     APP =  "Genomerepeatfinder"
#     logging.basicConfig(level=logging.INFO)
#     start_time = time.time()
#     sc = SparkContext(appName=APP)
#     spark = SparkSession(sc)
#     labs = spark.read.text("s3a://retail-employees/data/final_genome.fasta").rdd.map(lambda r: r[0])
#     pairs = labs.flatMap(repetition_finder)
#     frequencies = pairs.reduceByKey(lambda a, b: a+b)

#     logging.info(f"RESULT: {frequencies.collectAsMap()} seconds")
#     result = frequencies.collect()
#     for r in result:
#         print(r)


#     end_time = time.time()
#     execution_time = end_time - start_time
#     logging.info(f"time: {execution_time} seconds")

#     spark.stop()



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
