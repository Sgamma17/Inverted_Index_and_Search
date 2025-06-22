import os
import sys
import re
from collections import defaultdict

def normalize_line(line):
    return re.sub(r'[^a-z ]', ' ', line.lower())

def extract_word_file_pairs(file_path):
    filename = os.path.basename(file_path)
    with open(file_path, 'r', encoding='utf-8') as f:
        pairs = []
        for line in f:
            line = normalize_line(line)
            words = line.split()
            for word in words:
                if word:
                    pairs.append(((word, filename), 1))
        return pairs

def build_inverted_index(input_dir):
    all_pairs = []

    # Simula la flatMap con lista di tutte le (word, filename), 1
    for filename in os.listdir(input_dir):
        full_path = os.path.join(input_dir, filename)
        if os.path.isfile(full_path):
            pairs = extract_word_file_pairs(full_path)
            all_pairs.extend(pairs)

    # Simula reduceByKey: somma le occorrenze
    count_dict = defaultdict(int)
    for key, count in all_pairs:
        count_dict[key] += count

    # Simula il secondo map: (word, ["filename:count"])
    word_to_filenamecount = defaultdict(list)
    for (word, filename), count in count_dict.items():
        word_to_filenamecount[word].append(f"{filename}:{count}")

    # Simula reduceByKey finale: unisci liste
    sorted_index = sorted(word_to_filenamecount.items())

    return sorted_index

def write_output(inverted_index, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "part-00000")

    with open(output_path, "w", encoding="utf-8") as out_file:
        for word, filecounts in inverted_index:
            line = f"{word}\t" + "\t".join(filecounts)
            out_file.write(line + "\n")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: inverted_index_local.py <input dir> <output dir>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    index = build_inverted_index(input_dir)
    write_output(index, output_dir)
