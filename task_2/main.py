import string
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import requests
import matplotlib.pyplot as plt
import argparse


def get_text(url: str) -> str:
    try:
        response = requests.get(url)
        response.raise_for_status()  # Перевірка на помилки HTTP
        return response.text
    except requests.RequestException as e:
        print(f"Помилка завантаження тексту: {e}")
        return None

def remove_punctuation(text: str) -> str:
    return text.translate(str.maketrans("", "", string.punctuation))

def map_function(word: str) -> tuple:
    return word.lower(), 1

def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()

def reduce_function(key_values):
    key, values = key_values
    return key, sum(values)


def map_reduce(text: str) -> dict:
    # Видалення знаків пунктуації
    text = remove_punctuation(text)
    words = text.split()

    # Паралельний мапінг
    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))

    # Shuffle
    shuffled_values = shuffle_function(mapped_values)

    # Паралельна редукція
    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    return dict(reduced_values)


def visualize_top_words(word_freq, top_n=10):
    # Сортуємо слова за частотою використання
    sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:top_n]

    words, frequencies = zip(*sorted_words)

    # Побудова графіка
    plt.figure(figsize=(10, 6))
    plt.bar(words, frequencies, color='skyblue')
    plt.xlabel("Frequency")
    plt.ylabel("Words")
    plt.title(f"Top {top_n} words by frequency")
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    # Парсер аргументів командного рядка
    parser = argparse.ArgumentParser(description="Analysis of words frequency with MapReduce")
    parser.add_argument("--url", type=str, required=True, help="URL for text to analyze")
    parser.add_argument("--top", type=int, default=10, help="Number of top words to visualize, default=10")

    args = parser.parse_args()

    # Завантаження тексту
    url = args.url
    top_n = args.top

    text = get_text(url)

    if text:
        print("Teaxt loaded successfully!")
        # Виконання MapReduce
        word_frequencies = map_reduce(text)
        print("MapReduce completed!")

        # Візуалізація результатів
        visualize_top_words(word_frequencies, top_n=top_n)
    else:
        print("Error: Can't load text from URL.")
