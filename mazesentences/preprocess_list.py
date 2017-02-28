import csv
import pathlib
import json

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

def reprocess_distractors(file_in, file_out):
    file_in = pathlib.Path(file_in)

    with file_in.open('r', encoding='utf-8') as f:
        trials_in = json.load(f)['sentences']

        new_set = []
        count = 0
        bad_count = 0
        for trial in trials_in:
            bad = False
            # if 'sentence' not in trial:
            #     continue
            for pair in trial['sentence']:
                if len(pair) != 2:
                    bad = True
                    continue
                if bad:
                    continue
                if len(pair[0]) != len(pair[1]):
                    if 'Ｘ' in pair[1]:
                        pair[1] = 'Ｘ' * len(pair[0])
                    elif '＃' in pair[1]:
                        # print(pair[0].count(trial['critical_target']))
                        # print(trial['sentence'])

                        for key, distractor in trial['distractors'].items():
                            new_distractor = []
                            for char in pair[0]:
                                if char == trial['critical_target']:
                                     new_distractor.append(distractor)
                                else:
                                    new_distractor.append(char)
                            trial['distractors'][key] = ''.join(new_distractor)

                        # print(trial['distractors'])
                    else:
                        # print(trial['sentence'], '\n')
                        bad = True
                        bad_count += 1

            if not bad:
                count += 1
                trial['sentence_number'] = count
                new_set.append(trial)
                # print(trial)

        file_out = pathlib.Path(file_out)
        with file_out.open('w', encoding='utf-8') as f:
            json.dump({'sentences': new_set}, f, ensure_ascii=False, indent=2)


if __name__ == '__main__':
    # trials = read_list('data/new_sentences.txt')
    # write_trials(trials, 'data/generated_trials/trials_v110.json')
    reprocess_distractors('data/generated_trials/trials_v101.json', 'data/generated_trials/trials_v102.json')
