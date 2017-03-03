import csv
import pathlib
import json
import sys
import random

from .sentenceselector import Selector

def read_list(loc):
    sentence_file = pathlib.Path(loc)
    sentences = []
    with sentence_file.open('r', encoding='utf-8') as f:
        for line in f:
            sentences.append(line.strip())

    # random.shuffle(sentences)
    return sorted(sentences, key=lambda k: (len(k)-k.count(', ')))

def get_distractors(sentences, practice_file):
    selector = Selector('http://192.168.25.150:9200/', 'chinese_simplified', 2, min_year=1980)
    practice_file = pathlib.Path(practice_file)

    trials_list = []
    count = 0
    for sentence in sentences:
        count += 1
        full_sentence = selector.distractor_sentence(sentence, 'zzzz')
        processed_sentence = []

        for pair in full_sentence:
            if '*' in pair[1]:
                processed_sentence[-1][0] += pair[0]
                processed_sentence[-1][1] += pair[0]
            else:
                processed_sentence.append(pair)

        trial = {
            'sentence_number': count,
            'full_sentence': sentence,
            'sentence': processed_sentence
        }
        trials_list.append(trial)

        with practice_file.open('w', encoding='utf-8') as f:
            json.dump({'sentences': trials_list}, f, ensure_ascii=False, indent=2)

def main():
    trials = read_list('mazesentences/data/practice_sentences.txt')
    get_distractors(trials, 'mazesentences/data/practice_sentences.json')
