import csv
import pathlib
import json

def read_list(loc):
    trial_file = pathlib.Path(loc)
    with trial_file.open('r', encoding='utf-8') as f:
        sentences_list = json.load(f)['sentences']


    count = 0
    for sentence in sentences_list:
        count += 1

        sentence['sentence_number'] = count
        print(sentence['sentence_number'])

        for pair in sentence['sentence']:
            if '＃' in pair[1]:
                if len(pair[1]) > 1:
                    print(pair)


    print(json.dumps(sentences_list, indent=2, ensure_ascii=False))

if __name__ == '__main__':
    read_list('data/generated_trials/trials_v101.json')
