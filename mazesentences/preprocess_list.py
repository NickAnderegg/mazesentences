import csv
import pathlib
import json

from .sentenceselector import Selector

def read_list(loc):
    sentence_file = pathlib.Path(loc)
    with sentence_file.open('r', encoding='utf-8') as f:
        csvreader = csv.reader(f, delimiter='\t')

        groups = []
        curr_group = []
        for line in csvreader:

            if len(line):
                curr_group.append(line)
            else:
                groups.append(curr_group)
                curr_group = []

        groups.append(curr_group)

    # print(json.dumps(groups, indent=2, ensure_ascii=False))

    count = 0
    skipped = 0
    trials_list = []
    for group in groups:
        if len(group) == 5:
            skipped += 1
            continue

        count += 1

        trial = {
            'distractors': dict(),
            'sentence_number': count
        }
        if len(group[1]) == 1:
            # print('First crit: ', json.dumps(group, indent=2, ensure_ascii=False))
            trial['critical_target'] = group[0][0]
            trial['full_sentence'] = group[1][0]
            trial['distractors']['both_sim'] = group[2][0]
        elif len(group[2]) == 1:
            # print('Second crit: ', json.dumps(group, indent=2, ensure_ascii=False))
            trial['critical_target'] = group[1][0]
            trial['full_sentence'] = group[2][0]
            trial['distractors']['both_sim'] = group[0][0]

        trial['distractors']['orth_sim'] = group[3][0]
        trial['distractors']['phon_sim'] = group[4][0]
        trial['distractors']['both_dif'] = group[5][0]

        if '|' in trial['full_sentence']:
            # trial['full_sentence'] = ''.join(trial['full_sentence'].split('|'))
            trial['full_sentence'] = trial['full_sentence'].split('|')
            trial['combined_sentence'] = ''.join(trial['full_sentence'])

        # print(json.dumps(trial, indent=2, ensure_ascii=False))
        # print()

        trials_list.append(trial)
    # print(skipped)
    return trials_list

def write_trials(trials_list, trials_file):
    trials_file = pathlib.Path(trials_file)

    with trials_file.open('w', encoding='utf-8') as f:
        json.dump({'sentences': trials_list}, f, ensure_ascii=False, indent=2)

def pull_bad(file_in, bad_trials, bad_index):
    file_in = pathlib.Path(file_in)

    combos = {}
    for i in range(len(bad_trials)):
        combos[bad_trials[i]] = bad_index[i]

    print(combos)

    with file_in.open('r', encoding='utf-8') as f:
        trials_in = json.load(f)['sentences']

    selector = Selector('http://192.168.25.150:9200/', 'chinese_simplified', 2, min_year=1980)
    pulled_set = []
    for trial in trials_in:
        if trial['sentence_number'] in bad_trials:
            # print(json.dumps(trial, indent=2, ensure_ascii=False), '\n')
            print('Sentence num:', trial['sentence_number'])
            print('Sentence:')
            count = 0
            actual_index = 0
            for i in range(0, len(trial['sentence'])):
                if '*' in trial['sentence'][i][1]:
                    print('\t - \t', trial['sentence'][i])
                    continue

                if count != combos[trial['sentence_number']]:
                    print('\t', count, '\t', trial['sentence'][i])
                else:
                    print('\t', count, '\t\t', trial['sentence'][i])
                    actual_index = i

                count += 1

            new_distractors = selector.distractor_sentence(
                trial['full_sentence'],
                trial['critical_target']
            )

            print('New distractors:', new_distractors)

            if actual_index >= 3:
                back = 3
            elif actual_index >= 2:
                back = 2
            else:
                back = 1
            print('\nRevision:', actual_index)
            # print(trial['sentence'][actual_index-1:actual_index+1])
            print('Actual sequence / Distractor Sequence / Revised Sequence:\n',
                '{},'.format(' '.join(x[0] for x in trial['sentence'][actual_index-back:actual_index+1])),
                '{} {},'.format(' '.join(x[0] for x in trial['sentence'][actual_index-back:actual_index]), trial['sentence'][actual_index][1]),
                '{} {},'.format(' '.join(x[0] for x in trial['sentence'][actual_index-back:actual_index]), new_distractors[actual_index][1]),
                '{}, {}, {}'.format(trial['sentence'][actual_index-1][0], new_distractors[actual_index][0], new_distractors[actual_index][1])
            )
            print('\n\n',
                'Act: {},'.format(' '.join(x[0] for x in trial['sentence'][actual_index-back:actual_index+1])),
                'Dist:{} {},'.format(' '.join(x[0] for x in trial['sentence'][actual_index-back:actual_index]), trial['sentence'][actual_index][1]),
                'Rev: {} {}'.format(' '.join(x[0] for x in trial['sentence'][actual_index-back:actual_index]), new_distractors[actual_index][1])
                # '{}, {}, {}'.format(trial['sentence'][actual_index-1][0], new_distractors[actual_index][0], new_distractors[actual_index][1])
            )

            # print('\t\t', trial['sentence'][combos[trial['sentence_number']]])
            # for i in range(combos[trial['sentence_number']]+1, len(trial['sentence'])):
            #     print('\t', trial['sentence'][i])
            print('\n\n')

def reprocess_distractors(file_in, file_out):
    file_in = pathlib.Path(file_in)

    selector = Selector('http://192.168.25.150:9200/', 'chinese_simplified', 2, min_year=1980)

    with file_in.open('r', encoding='utf-8') as f:
        trials_in = json.load(f)['sentences']

        new_set = []
        count = 0
        bad_count = 0
        bad_trials = []
        updated_dist, correct_dist = 0, 0
        for trial in trials_in:
            bad = False
            saw_critical = False
            for pair in trial['sentence']:
                if bad:
                    continue
                if len(pair) != 2:
                    bad = True
                    bad_count += 1
                    bad_trials.append(trial['sentence_number'])
                    continue
                if saw_critical and '＃' in pair[1]:
                    print('\nDouble critical markers:', trial['sentence'], '\n')
                    bad = True
                    bad_count += 1
                    bad_trials.append(trial['sentence_number'])
                    continue
                elif ('.' in pair[0] or ',' in pair[0]) and pair[1] != '*':
                    print('\nMisaligned punctuation:', trial['sentence'], '\n')
                    bad = True
                    bad_count += 1
                    bad_trials.append(trial['sentence_number'])
                    continue
                elif '＃' in pair[1]:
                    # print(pair[0].count(trial['critical_target']))
                    # print(trial['sentence'])

                    if len(pair[0]) > 1:
                        modify_distractors = False
                        for key, distractor in trial['distractors'].items():
                            if modify_distractors:
                                break
                            if len(distractor) != len(pair[0]):
                                modify_distractors = True
                                break

                            for i in range(len(pair[0])):
                                if pair[0][i] == distractor[i] or pair[0][i] == trial['critical_target']:
                                    continue
                                else:
                                    modify_distractors = True
                                    break

                        if not modify_distractors:
                            # print('\nDistractors already correct...')
                            # print('Critical char:', trial['critical_target'])
                            # print('Critical word:', pair[0])
                            # print('Distractors:', trial['distractors'], '\n')
                            correct_dist += 1
                            continue

                        for key, distractor in trial['distractors'].items():
                            new_distractor = []
                            for char in pair[0]:
                                if char == trial['critical_target']:
                                     new_distractor.append(distractor)
                                else:
                                    new_distractor.append(char)
                            trial['distractors'][key] = ''.join(new_distractor)

                        # print('\nUpdated distractors...')
                        # print('Critical char:', trial['critical_target'])
                        # print('Critical word:', pair[0])
                        # print('Distractors:', trial['distractors'], '\n')
                        updated_dist += 1

                    saw_critical = True
                elif len(pair[0]) != len(pair[1]):
                    if 'Ｘ' in pair[1]:
                        pair[1] = 'Ｘ' * len(pair[0])
                    elif '＃' in pair[1]:
                        for key, distractor in trial['distractors'].items():
                            if bad:
                                continue
                            if len(pair[0]) != len(distractor):
                                print('\nInvalid distractor...')
                                print('Critical char:', trial['critical_target'])
                                print('Critical word:', pair[0])
                                print('Distract word:', distractor, '\n')
                                bad = True
                                bad_count += 1
                                bad_trials.append(trial['sentence_number'])
                                continue
                            for i in range(len(pair[0])):
                                if pair[0][i] != distractor[i] and pair[0][i] != trial['critical_target']:
                                    print('\nInvalid distractor...')
                                    print('Critical char:', trial['critical_target'])
                                    print('Critical word:', pair[0])
                                    print('Distract word:', distractor, '\n')
                                    bad = True
                                    bad_count += 1
                                    bad_trials.append(trial['sentence_number'])
                                    continue
                    else:
                        print(trial['sentence'], '\n')
                        bad = True
                        bad_count += 1
                        bad_trials.append(trial['sentence_number'])

            if not bad:
                count += 1
                trial['sentence_number'] = count
                new_set.append(trial)
                # print(trial)
            else:
                new_distractors = selector.distractor_sentence(
                    trial['full_sentence'],
                    trial['critical_target']
                )
                trial['sentence'] = new_distractors
                print(trial)
                new_set.append(trial)

        print('Good:', count, 'Bad:', bad_count)
        print('Updated distractors:', updated_dist, 'Correct distractors:', correct_dist)
        print('Bad trials:', bad_trials)

        file_out = pathlib.Path(file_out)
        with file_out.open('w', encoding='utf-8') as f:
            json.dump({'sentences': new_set}, f, ensure_ascii=False, indent=2)


def main():
    # trials = read_list('data/new_sentences.txt')
    # write_trials(trials, 'data/generated_trials/trials_v110.json')
    reprocess_distractors('mazesentences/data/generated_trials/trials_v103-modified_ambiguous.json', 'mazesentences/data/generated_trials/trials_v103.1.json')
    # pull_bad('data/generated_trials/trials_v102.json',
    #     [34,26,75,56,18,25,38],
    #     [ 1, 5, 4, 8, 2, 1, 1]
    # )

    # [34,26,75,58,27,71,56,18,15,49,10,25, 1,38],
    # [ 1, 5, 4, 3, 3, 5, 8, 2, 5, 2, 3, 1, 1, 1]

    # [58,27,15,49,10, 1,71],
    # [ 3, 3, 5, 2, 3, 1, 5]

if __name__ == '__main__':
    main()
