import csv
import pathlib
import json

from .sentenceselector import Selector

def read_file():
    path = pathlib.Path('mazesentences/data/stimulus_set-02.csv')

    stimuli = dict()

    with path.open('r', encoding='utf-8', newline='') as f:
        csvreader = csv.reader(f, delimiter='\t')

        count = 0
        critical = None
        for line in csvreader:

            if count % 6 == 0:
                critical = line[0]
                stimuli[critical] = dict()
            elif count % 6 == 1:
                stimuli[critical]['both_sim'] = line[0]
            elif count % 6 == 2:
                stimuli[critical]['orth_sim'] = line[0]
            elif count % 6 == 3:
                stimuli[critical]['phon_sim'] = line[0]
            elif count % 6 == 4:
                stimuli[critical]['both_dif'] = line[0]

            count += 1

    return stimuli

def get_sentences():
    stimuli = read_file()

    trials = {'sentences': []}
    count = 0
    for critical, distractors in stimuli.items():
        count += 1
        try:
            trial = dict()
            trial['sentence_number'] = count
            trial['critical_target'] = critical
            trial['distractors'] = distractors

            selector = Selector('http://192.168.1.150:9200/', 'chinese_simplified', 2, min_year=1980)

            sentences = selector.get_sentences(critical, max_sentences=1000)

            if sentences is None:
                continue

            sentence = sentences[0]
            trial['full_sentence'] = sentence[0]
            trial['sentence'] = selector.distractor_sentence(sentence[0], critical)
        except:
            continue

        # print(json.dumps(trial, ensure_ascii=False, indent=2))
        # print()

        trials['sentences'].append(trial)

        print('Created trial #{}'.format(count))

        path = pathlib.Path('mazesentences/data/trial.json')
        with path.open('w', encoding='utf-8') as f:
            json.dump(trials, f, ensure_ascii=False, indent=2)

def _get_trials_file():
    trials_file = pathlib.Path('mazesentences/data/trialstest_v001.json')
    print(trials_file)
    while trials_file.exists():
        base_name, prev_version = tuple(trials_file.name.split('_'))
        prev_version = int(prev_version[1:])

        curr_version += 1

        trials_file = pathlib.Path(
            '{}/{}_v{:0>d}'.format(
                trials_file.parent,
                base_name,
                curr_version
            )
        )

        print(trials_file)

    trials_file.open('w', encoding='utf-8')
