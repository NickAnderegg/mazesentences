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

def reprocess_trials():
    trials_file = _get_trials_file(increment=False)

    if not trials_file.exists():
        return

    with trials_file.open('r', encoding='utf-8') as f:
        trials_list = json.load(f)
        trials_list = trials_list['sentences']

    bad_trials = []
    rejected_sentences = []
    checked_stimuli = set()
    for ix, trial in enumerate(trials_list):

        if trial['critical_target'] in checked_stimuli:
            bad_trials.append(ix)
            print('Duplicate trial found...')
            continue

        checked_stimuli.add(trial['critical_target'])

        contains_ascii = False
        for char in list(trial['full_sentence']):
            if ord(char) < 128 and unicodedata.category(char)[0] != 'P':
                contains_ascii = True
                break
        if contains_ascii:
            bad_trials.append(ix)
            print('Sentence #{} contains ascii'.format(ix+1))
            continue

        if 'new_sentence' in trial and trial['new_sentence'] is True:
            bad_trials.append(ix)
            rejected_sentences.append(trial['full_sentence'])
            print('Sentence #{} marked for regeneration'.format(ix+1))
            continue

        if trial['full_sentence'].index(trial['critical_target']) >= len(trial['full_sentence']) - 3:
            print('Sentence #{} has late critical char appearance'.format(ix+1))
            bad_trials.append(ix)
            continue

    for t in sorted(bad_trials, reverse=True):
        print(trials_list.pop(t))

    trials = {'sentences': trials_list}

    trials_file = _get_trials_file()

    with trials_file.open('w', encoding='utf-8') as f:
        json.dump(trials, f, ensure_ascii=False, indent=2)

    rejected_sentences_file = pathlib.Path('mazesentences/data/rejected_sentences.txt')
    with rejected_sentences_file.open('a', encoding='utf-8') as f:
        f.write('\n'.join(rejected_sentences) + '\n')

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
        poss_replace = trials[stim]['distractors']['both_sim']
        if poss_replace in stimuli and stimuli[poss_replace]['both_sim'] == stim:
            print('Stimuli swap found for {} and {}'.format(poss_replace, stim))

            stimuli[stim] = stimuli[poss_replace].copy()
            stimuli[stim]['both_sim'] = poss_replace
            del stimuli[poss_replace]
            continue
        del trials[stim]
        # print('Skipping {}...'.format(stim))

    incomplete_critical = list(set(stimuli) - set(trials))
    incomplete_sets = dict((crit, stimuli[crit]) for crit in incomplete_critical)

    trials_list = list(trials.values())
    random.shuffle(trials_list)
    count = 0
    for t in trials_list:
        del t['sentence_number']

        count += 1
        t['sentence_number'] = count

    return incomplete_sets, trials_list, stimuli.keys()

def get_sentences():
    stimuli, trials, full_set = _get_incomplete_sets()

    trials_file = _get_trials_file()

    # print(len(stimuli), len(trials))

    if trials is None:
        trials = {'sentences': []}
        count = 0
        print('Loaded {} extant trials, {} stimuli to process...'.format(count, len(stimuli)))
    else:
        count = len(trials)
        print('Loaded {} extant trials, {} stimuli to process...'.format(count, len(stimuli)))
        trials = {'sentences': trials}

        with trials_file.open('w', encoding='utf-8') as f:
            json.dump(trials, f, ensure_ascii=False, indent=2)

    for critical, distractors in stimuli.items():
        count += 1
        try:
            trial = dict()
            trial['sentence_number'] = count
            trial['critical_target'] = critical
            trial['distractors'] = distractors

            selector = Selector('http://192.168.25.150:9200/', 'chinese_simplified', 2, min_year=1980)
            # selector = Selector('http://home.anderegg.io:9292/', 'chinese_simplified', 2, min_year=1980)

            sentences = selector.get_sentences(critical, max_sentences=1000)

            if sentences and len(sentences) < 15:
                too_few = True
            else:
                too_few = False
            if not sentences or too_few:
                new_critical = distractors['both_sim']
                if new_critical not in stimuli and new_critical not in full_set:
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
                    if not sentences and too_few:
                        new_critical = distractors['both_sim']
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
                    elif not sentences:
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

def regenerate_distractors():
    trials_file = _get_trials_file(increment=False)
    new_trials_file = _get_trials_file()

    with trials_file.open('r', encoding='utf-8') as f:
        trials_list = json.load(f)
        trials_list = trials_list['sentences']

    trials = []
    for t in trials_list:
        trials.append({
            key: value
            for key, value in t.items()
            if key != 'sentence'
        })

    trials = {'sentences': trials}

    selector = Selector('http://192.168.25.150:9200/', 'chinese_simplified', 2, min_year=1980)
    # selector = Selector('http://home.anderegg.io:9292/', 'chinese_simplified', 2, min_year=1980)
    sentence_number = 0
    for trial in trials['sentences']:
        sentence_number += 1
        trial['sentence_number'] = sentence_number

        trial['sentence'] = selector.distractor_sentence(
            trial['full_sentence'],
            trial['critical_target']
        )

        # print(json.dumps(trials[sentence_number-1], indent=2, ensure_ascii=False))

        print('Created trial #{}'.format(sentence_number))

        with new_trials_file.open('w', encoding='utf-8') as f:
            json.dump(trials, f, ensure_ascii=False, indent=2)

    # print(json.dumps(trials[:3], indent=2, ensure_ascii=False))

def _get_trials_file(increment=True):
    trials_file = pathlib.Path('mazesentences/data/generated_trials/trials_v001.json')
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

def generate_sample(n, choices=None):
    trials_file = _get_trials_file(increment=False)

    with trials_file.open('r', encoding='utf-8') as f:
        trials_list = json.load(f)
        trials_list = trials_list['sentences']

    if not choices:
        choices = random.sample(range(1, len(trials_list)+1), n)

    subset = []
    for choice in choices:
        subset.append(trials_list[choice-1])

    file_output = []

    file_output.append('========\nSample Trials:\n========\n')

    for trial in subset:
        file_output.append('--------\n')
        file_output.append('Trial: {}\n'.format(trial['sentence_number']))
        file_output.append('Critical: {} / Distractors – Both: {} | Orth: {} | Phon: {} | Diff: {}\n\n'.format(
            trial['critical_target'],
            trial['distractors']['both_sim'],
            trial['distractors']['orth_sim'],
            trial['distractors']['phon_sim'],
            trial['distractors']['both_dif']
        ))

        file_output.append('Full sentence: {}\n\n'.format(trial['full_sentence']))
        target_sentence = []
        distractor_sentence = []
        print('Arranging sententence', trial['sentence_number'])
        for pair in trial['sentence']:
            if 'Ｘ' in pair[1]:
                pair[1] = ''.join(['Ｘ']*len(pair[0]))
            if pair[1] == '*':
                target_sentence[-1] += pair[0]
                distractor_sentence[-1] += pair[0]
            else:
                target_sentence.append(pair[0])
                distractor_sentence.append(pair[1])

        target_sentence = ' | '.join(target_sentence)
        distractor_sentence = ' | '.join(distractor_sentence)

        if len(target_sentence) > 55:
            try:
                break_point = target_sentence[55:].index(' | ') + 55

                target_sentence = (target_sentence[:55],  target_sentence[57:])
                distractor_sentence = (distractor_sentence[:55], distractor_sentence[57:])
            except:
                pass

        if type(target_sentence) is str:
            file_output.append('Target: {}\n'.format(target_sentence))
            file_output.append('Alter.: {}\n\n'.format(distractor_sentence))
        else:
            file_output.append('Target: {} ->\n'.format(target_sentence[0]))
            file_output.append('Alter.: {} ->\n\n'.format(distractor_sentence[0]))

            file_output.append('Target (cont.): {}\n'.format(target_sentence[1]))
            file_output.append('Alter. (cont.): {}\n\n'.format(distractor_sentence[1]))

    sample_file = pathlib.Path('mazesentences/data/trial_samples/trial_samples{}.txt'.format(trials_file.stem[-5:]))
    with sample_file.open('w', encoding='utf-8') as f:
        f.writelines(file_output)

def generate_sentences_raw():
    trials_file = _get_trials_file(increment=False)

    with trials_file.open('r', encoding='utf-8') as f:
        trials_list = json.load(f)
        trials_list = trials_list['sentences']

    file_output = []

    for trial in trials_list:
        file_output.append('Sentence: {}, Critical: {}\n{}~ {}\n\n'.format(trial['sentence_number'], trial['critical_target'], trial['sentence_number'], trial['full_sentence']))

    sentences_file = pathlib.Path('mazesentences/data/sentences_raw/sentences_raw{}.txt'.format(trials_file.stem[-5:]))
    with sentences_file.open('w', encoding='utf-8') as f:
        f.writelines(file_output)

def recombine_sentences(n):
    raw_file = pathlib.Path('mazesentences/data/sentences_raw/sentences_raw_v{:0>3}.txt'.format(n))
    translated_file = pathlib.Path('mazesentences/data/sentences_translated/sentences_translated_v{:0>3}.txt'.format(n))

    with raw_file.open('r', encoding='utf-8') as f_raw:
        raw_lines = {}
        for line in f_raw:
            if '~' in line:
                num, sentence = tuple(line.strip().split('~'))
                raw_lines[int(num.strip())] = sentence.strip()

    with translated_file.open('r', encoding='utf-8') as f_translated:
        translated_lines = {}
        for line in f_translated:
            if len(line) > 1:
                num, sentence = tuple(line.strip().split('~ '))
                translated_lines[int(num)] = sentence

    combined_file = pathlib.Path('mazesentences/data/sentences_combined/sentences_combined_v{:0>3}.txt'.format(n))
    with combined_file.open('w', encoding='utf-8') as f:
        for num in raw_lines:
            f.write('Sentence {:0>3}:\n{}\n{}\n\n'.format(num, raw_lines[num], translated_lines[num]))
