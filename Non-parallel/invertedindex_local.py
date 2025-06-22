import os
import sys
import re
import time
import psutil
from collections import defaultdict

def normalize_line(line):
    return re.sub(r'[^a-z]', ' ', line.lower())

def extract_word_file_pairs(file_path):
    filename = os.path.basename(file_path)
    with open(file_path, 'r', encoding='utf-8') as f:
        pairs = defaultdict(int)
        num_lines = f.readlines().__len__()
        cur_line = 0

        f.seek(0)
        for line in f:
            line = normalize_line(line)
            words = line.split()
            for word in words:
                if word:
                    key = word + "@" + filename
                    pairs[key] += 1

            cur_line += 1
            avanzamento = (cur_line / num_lines) * 100
            print(f"\r    --> Avanzamento in file {filename}: {avanzamento:.2f}%", end="")
        print("\n")
        return pairs

def build_inverted_index(input_dir):
    all_pairs = defaultdict(int)

    # Simula la flatMap con lista di tutte le (word, filename), 1
    print("\n------------ MAP ------------\n")
    num_files = len(os.listdir(input_dir))
    cur_file = 0
    for filename in os.listdir(input_dir):
        cur_file += 1
        print(f"Processing file {cur_file}/{num_files}\n", end="")

        full_path = os.path.join(input_dir, filename)
        if os.path.isfile(full_path):
            pairs = extract_word_file_pairs(full_path)
            print(f"num_pairs: {len(pairs)}\n")
            for key, value in pairs.items():
                all_pairs[key] += value
    print("\nCompletata la map().\n")

    # Simula il secondo map: (word, ["filename:count"])
    print("\n------------ SECONDA MAP ------------\n")
    word_to_filenamecount = defaultdict(list)
    num_counts = len(all_pairs)
    cur_count = 0
    for key, value in all_pairs.items():
        cur_count += 1
        print(f"\r    --> Avanzamento nella seconda map: {cur_count}/{num_counts}", end="")

        word = key.split('@')[0]
        filename = key.split('@')[1]
        word_to_filenamecount[word].append(f"{filename}:{value}")
    print("\nCompletata la seconda map().\n")

    # Simula reduceByKey finale: unisci liste
    print("\n------------ SORT ------------\n")
    sorted_index = sorted(word_to_filenamecount.items())
    print("Completata la sort().\n")

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
    start_time = time.time()

    process = psutil.Process(os.getpid())
    initial_memory = process.memory_info().rss / (1024 * 1024)  # Convert to MB

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    index = build_inverted_index(input_dir)
    write_output(index, output_dir)

    end_time = time.time()
    elapsed_time = end_time - start_time

    final_memory = process.memory_info().rss / (1024 * 1024)  # Convert to MB
    memory_used = final_memory - initial_memory

    print(f"\nInverted index built in {elapsed_time:.2f} seconds.")
    print(f"Memory used: {memory_used:.2f} MB.")
