from pathlib import Path
import json
import csv
import pprint

trials_file = Path('data/generated_trials/trials_v048.json')
stims_file = Path('data/stimulus_set-02.csv')

orig_trials = json.load(trials_file.open('r'))['sentences']

trial_sets = {}
for trial in orig_trials:
    trial_key = frozenset([trial['critical_target']] + [x for x in trial['distractors'].values()])
    trial_sets[trial_key] = {
        'critical': trial['critical_target'],
        'distractors': trial['distractors']
    }

# print(pprint.pformat(trial_sets, indent=2, width=120))
# print()
# quit()

with stims_file.open('r') as f:
    csvreader = csv.reader(f, delimiter='\t')
    orig_stims = {}

    trial_key = []
    trial_data = {}
    for ix, row in enumerate(csvreader):
        if ix % 6 == 0:
            trial_data['critical'] = tuple(row)
            trial_data['distractors'] = {}
            trial_data['pronunciation'] = {}

            trial_key.append(row[0])
        elif ix % 6 == 1:
            trial_data['distractors']['both_sim'] = tuple(row)

            trial_key.append(row[0])
        elif ix % 6 == 2:
            trial_data['distractors']['orth_sim'] = tuple(row)

            trial_key.append(row[0])
        elif ix % 6 == 3:
            trial_data['distractors']['phon_sim'] = tuple(row)

            trial_key.append(row[0])
        elif ix % 6 == 4:
            trial_data['distractors']['both_dif'] = tuple(row)

            trial_key.append(row[0])
        elif ix % 6 == 5:
            # print(trial_data)
            orig_stims[frozenset(trial_key)] = trial_data

            trial_key = []
            trial_data = {}

# print(pprint.pformat(orig_stims, indent=2, width=120))
quit()
new_stims_file = Path('data/stimulus_set-03.csv')
with new_stims_file.open('w', encoding='utf-8') as f:
    csvwriter = csv.writer(f, delimiter='\t', lineterminator='\n')

    count = 0
    for trial, value in orig_stims.items():
        if trial in trial_sets:
            count += 1
            csvwriter.writerow(value['critical'])
            csvwriter.writerow(value['distractors']['both_sim'])
            csvwriter.writerow(value['distractors']['orth_sim'])
            csvwriter.writerow(value['distractors']['phon_sim'])
            csvwriter.writerow(value['distractors']['both_dif'])
            csvwriter.writerow('')

    print('Total:', count)

    count += 0
    for trial, value in trial_sets.items():
        if trial not in orig_stims:
            count += 1
            csvwriter.writerow(value['critical'])
            csvwriter.writerow(value['distractors']['both_sim'])
            csvwriter.writerow(value['distractors']['orth_sim'])
            csvwriter.writerow(value['distractors']['phon_sim'])
            csvwriter.writerow(value['distractors']['both_dif'])
            csvwriter.writerow('')

    print('Total:', count)
