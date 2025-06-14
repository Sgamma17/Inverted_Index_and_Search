import sys
import os

def load_inverted_index(output_dir):
    # Carica l'indice dai file part-* presenti nella directory output_dir
    
    index = {}

    if not os.path.exists(output_dir):
        print(f"Error: Output directory '{output_dir}' does not exist.")
        sys.exit(1)

    for fileName in os.listdir(output_dir):
        if fileName.startswith("part-"):
            with open(os.path.join(output_dir, fileName), "r", encoding="utf-8") as file:
                for line in file:
                    try:
                        word, locations = line.strip().split("\t", 1)
                        file_list = []
                        for entry in locations.split("\t"):
                            file_path = entry.rsplit(":", 1)[0]
                            file_list.append(file_path)
                        index[word] = sorted(set(file_list))
                    except ValueError:
                        continue  # Salta le righe che non hanno il formato corretto
    return index

def search_phrase(index, phrase):
    words = phrase.strip().lower().split()
    if not words:
        return set()
    
    if words[0] not in index:
        return set()
    result = set(index[words[0]])

    for word in words[1:]:
        if word not in index:
            return set()
        result = result.intersection(index[word])

    return result


def interactive_search(index):
    print("\nInverted index ready. type a query to search or 'qq' to quit:")
    while True:
        query = input(">> ").strip().lower()
        if(query == "qq"):
            print("Exiting search.")
            break
        files = search_phrase(index, query)
        if files:
            print(f"\nFiles containing '{query}':")
            for file in sorted(files):
                print(f"- {file}")
            print()
        else:
            print(f"No files found containing '{query}'.\n")

def main():
    if len(sys.argv) != 2:
        print("Usage: python search_query.py <output_dir>")
        sys.exit(1)

    output_dir = sys.argv[1]

    inverted_index = load_inverted_index(output_dir)
    interactive_search(inverted_index)

if __name__ == "__main__":
    main()
