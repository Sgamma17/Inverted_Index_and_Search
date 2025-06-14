from pyspark import SparkConf, SparkContext
import sys
import re
from collections import defaultdict

def normalize_line(line):
    # Lowercase and remove punctuation
    return re.sub(r'[^a-z0-9 ]', ' ', line.lower())

def tokenize(file_content):
    filename, lines = file_content
    results = []
    for line in lines.split('\n'):
        line = normalize_line(line)
        words = line.strip().split()
        for word in words:
            if word:
                results.append(((word, filename), 1))
    return results

def format_output(word_file_counts):
    word, occurrences = word_file_counts
    file_list = [f"{filename}:{count}" for filename, count in occurrences]
    return (word, "\t".join(file_list))


def extract_file_name(file_path):
    return file_path.split('/')[-1]


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit inverted_index_spark.py <input_dir> <output_dir>")
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    conf = SparkConf().setAppName("Inverted Index")
    sc = SparkContext(conf=conf)

    # Read all files as (filename, content)
    files = sc.wholeTextFiles(input_path)

    # Map phase: ((word, filename), 1)
    word_file_pairs = files.flatMap(
        lambda x: [((word, extract_file_name(x[0])), 1) for word in x[1].lower().split()]
    )



    # Combine and reduce by key: ((word, filename), count)
    word_file_counts = word_file_pairs.reduceByKey(lambda x, y: x + y)

    # Rearrange as (word, (filename, count))
    word_to_file_count = word_file_counts.map(lambda x: (x[0][0], f"{x[0][1]}: {x[1]}")).groupByKey().mapValues(lambda vals: "\t".join(vals))
    # Group by word: (word, [(file1, count1), (file2, count2), ...])
   # grouped = word_to_file_count.groupByKey().mapValues(list)

    # Format final output: (word, "file1:count1\tfile2:count2")
    result = word_to_file_count.sortByKey()

    # Save result
    result.saveAsTextFile(output_path)
    sc.stop()
