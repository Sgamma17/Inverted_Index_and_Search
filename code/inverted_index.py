from pyspark import SparkConf, SparkContext
import sys
import re

# Funzione per normalizzare una riga di testo:
# converto tutto in minuscolo e rimuovo caratteri non alfabetici (restano solo lettere e spazi)
def normalize_line(line):
    return re.sub(r'[^a-z ]', ' ', line.lower())

# Estraggo solo il nome del file da un path completo
def extract_filename(path):
    return path.split('/')[-1]

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit inverted_index_spark.py <input_dir> <output_dir>")
        sys.exit(-1)

    # Prendo in input la directory dei file e la directory in cui salvare l’inverted index
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    # Creo una configurazione Spark e inizializzo il contesto
    conf = SparkConf().setAppName("Inverted Index Spark")
    sc = SparkContext(conf=conf)

    # Leggo tutti i file nella directory: ottengo RDD di (file_path, contenuto_testo)
    files = sc.wholeTextFiles(input_path)

    # Estraggo le coppie ((parola, nome_file), 1) per ogni parola normalizzata
    word_file_pairs = files.flatMap(lambda file: [
        ((word, extract_filename(file[0])), 1)      # creo la coppia con count 1
        for line in file[1].split('\n')             # divido il testo in righe
        for word in normalize_line(line).split()    # normalizzo e divido in parole
        if word                                     # escludo stringhe vuote
    ])

    # Sommo tutte le occorrenze della stessa parola nello stesso file
    word_file_counts = word_file_pairs.reduceByKey(lambda a, b: a + b)

    # Cambio struttura: da ((word, file), count) a (word, "file1:count")
    # raggruppo tutte le occorrenze per parola, ordino e unisco come stringa, ordino le parole in ordine alfabetico
    inverted_index = word_file_counts \
        .map(lambda x: (x[0][0], f"{x[0][1]}:{x[1]}")) \
        .groupByKey() \
        .mapValues(lambda files: "\t".join(sorted(files))) \
        .sortByKey()

    # Salvo l’inverted index come file di testo, con formato: parola \t file1:count \t file2:count
    inverted_index.map(lambda x: f"{x[0]}\t{x[1]}").saveAsTextFile(output_path)

    # Termino il contesto Spark
    sc.stop()

