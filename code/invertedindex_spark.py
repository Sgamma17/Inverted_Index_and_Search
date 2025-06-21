import sys
from pyspark import SparkConf, SparkContext
import re
import os

def normalize_line(line):
    return re.sub(r'[^a-z ]', ' ', line.lower())

def extract_word_file_pairs(filename_content):
    full_path, content = filename_content
    filename = os.path.basename(full_path)
    content = normalize_line(content)
    words = content.split()
    return [((word, filename), 1) for word in words]

if __name__ == "__main__":

    if len(sys.argv) < 3:
        print("Usage: inverted_index.py <input dir> <output dir>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("Inverted Index Spark")
    sc = SparkContext(conf=conf)

    files_rdd = sc.wholeTextFiles(sys.argv[1])

    word_file_pairs = files_rdd.flatMap(extract_word_file_pairs)

    word_file_counts = word_file_pairs.reduceByKey(lambda x, y: x + y)

    word_to_filenamecount = word_file_counts.map(lambda x: (x[0][0], [f"{x[0][1]}:{x[1]}"]))

    inverted_index = word_to_filenamecount.reduceByKey(lambda a, b: a + b)

    output_lines = inverted_index.map(lambda x: f"{x[0]}\t" + "\t".join((x[1])))

    output_lines = output_lines.sortBy(lambda line: line.split("\t")[0])

    output_lines.repartition(1).saveAsTextFile(sys.argv[2])

