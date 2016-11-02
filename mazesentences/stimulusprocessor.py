import csv
import pathlib
import json
import random
from operator import itemgetter
import unicodedata

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

def _reprocess_trials():
    trials_file = _get_trials_file(increment=False)

    if not trials_file.exists():
        return

    with trials_file.open('r', encoding='utf-8') as f:
        trials_list = json.load(f)
        trials_list = trials_list['sentences']

    bad_trials = []
    for ix, trial in enumerate(trials_list):

        contains_ascii = False
        for char in list(trial['full_sentence']):
            if ord(char) < 128 and unicodedata.category(char)[0] != 'P':
                contains_ascii = True
                break
        if contains_ascii:
            bad_trials.append(ix)
            continue

        if trial['full_sentence'].index(trial['critical_target']) >= len(trial['full_sentence']) - 3:
            bad_trials.append(ix)

    for t in sorted(bad_trials, reverse=True):
        print(trials_list.pop(t))

    trials = {'sentences': trials_list}

    trials_file = _get_trials_file()

    with trials_file.open('w', encoding='utf-8') as f:
        json.dump(trials, f, ensure_ascii=False, indent=2)

    print('Dumped {} trials...'.format(len(bad_trials)))

def _get_incomplete_sets():
    stimuli = read_file()
    trials_file = _get_trials_file(increment=False)

    if not trials_file.exists():
        return stimuli, None

    with trials_file.open('r', encoding='utf-8') as f:
        trials_list = json.load(f)
        trials_list = trials_list['sentences']

    trials = dict()
    for t in trials_list:
        trials[t['critical_target']] = t

    for stim in (set(trials) - set(stimuli)):
        del trials[stim]

    incomplete_critical = list(set(stimuli) - set(trials))
    incomplete_sets = dict((crit, stimuli[crit]) for crit in incomplete_critical)

    trials_list = list(trials.values())
    random.shuffle(trials_list)
    count = 0
    for t in trials_list:
        del t['sentence_number']

        count += 1
        t['sentence_number'] = count

    return incomplete_sets, trials_list

def get_sentences():
    stimuli, trials = _get_incomplete_sets()

    trials_file = _get_trials_file()

    # print(len(stimuli), len(trials))

    if trials is None:
        trials = {'sentences': []}
        count = 0
    else:
        count = len(trials)
        print('Loaded {} extant trials...'.format(count))
        trials = {'sentences': trials}

    for critical, distractors in stimuli.items():
        count += 1
        try:
            trial = dict()
            trial['sentence_number'] = count
            trial['critical_target'] = critical
            trial['distractors'] = distractors

            selector = Selector('http://192.168.1.150:9200/', 'chinese_simplified', 2, min_year=1980)

            sentences = selector.get_sentences(critical, max_sentences=1000)

            if not sentences:
                new_critical = distractors['both_sim']
                if new_critical not in stimuli:
                    print('Attempting flipped pair')
                    print('Old critical:', critical)
                    print('Old distractors:', distractors)
                    trial = dict()
                    trial['sentence_number'] = count
                    trial['critical_target'] = new_critical

                    new_distractors = distractors.copy()
                    new_distractors['both_sim'] = critical
                    trial['distractors'] = new_distractors

                    critical = new_critical
                    distractors = new_distractors
                    print('New critical:', critical)
                    print('New distractors:', distractors)

                    sentences = selector.get_sentences(critical, max_sentences=1000)
                    if not sentences:
                        continue
                else:
                    print('Cannot attempt flipped pair. Continuing...')
                    continue

            sentence = random.choice(sentences)
            trial['full_sentence'] = sentence[0]
            trial['sentence'] = selector.distractor_sentence(sentence[0], critical)
        except KeyboardInterrupt:
            quit()
        except:
            continue

        # print(json.dumps(trial, ensure_ascii=False, indent=2))
        # print()

        trials['sentences'].append(trial)

        print('Created trial #{}'.format(count))

        with trials_file.open('w', encoding='utf-8') as f:
            json.dump(trials, f, ensure_ascii=False, indent=2)

def _get_trials_file(increment=True):
    trials_file = pathlib.Path('mazesentences/data/trials_v001.json')
    if increment is False:
        last_file = pathlib.Path(trials_file)

    # print(trials_file)
    while trials_file.exists():
        if increment is False:
            last_file = pathlib.Path(trials_file)

        base_name, prev_version = tuple(trials_file.stem.split('_'))
        prev_version = int(prev_version[1:])

        curr_version = prev_version + 1

        trials_file = pathlib.Path(
            '{}/{}_v{:0>3}.json'.format(
                trials_file.parent,
                base_name,
                curr_version
            )
        )
        # print(trials_file)

    if increment is False:
        # print('Returning: ', last_file)
        return last_file
    else:
        # print('Returning: ', trials_file)
        return trials_file
