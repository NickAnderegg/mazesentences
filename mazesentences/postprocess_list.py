import csv
import pathlib
import json
from operator import itemgetter

class TrialList(object):
    def __init__(self, loc=None):
        self.trials = {}

        if loc:
            self._load_file(loc)

    def _load_file(self, loc):
        trial_file = pathlib.Path(loc)
        with trial_file.open('r', encoding='utf-8') as f:
            sentences_list = json.load(f)['sentences']

        if len(self.trials) == 0:
            count = 0
        else:
            count = len(self.trials)
        for sentence in sentences_list:
            count += 1
            sentence['sentence_number'] = count
            self.trials[count] = sentence

    def add_file(self, loc):
        self._load_file(loc)
        # self.renumber_sentences()

    def check_duplicates(self):
        sentence_set = set()
        for key, value in self.trials.items():
            if value['full_sentence'][:5] in sentence_set:
                print('Duplicate #{}: {}'.format(key, value['full_sentence']))
            sentence_set.add(value['full_sentence'][:5])

    def renumber_sentences(self):
        # trials_list = list(self.trials.values())

        print(json.dumps(self.trials, indent=2, ensure_ascii=False))
        print(len(self.trials))

    def write_file(self, loc):
        # full_set = [None] * len(self.trials)
        full_set = []
        for key, value in self.trials.items():
            # full_set[key-1] = value
            full_set.append(value)

        file_out = pathlib.Path(loc)
        with file_out.open('w', encoding='utf-8') as f:
            json.dump({'sentences': sorted(full_set, key=itemgetter('sentence_number'))}, f, ensure_ascii=False, indent=2)

    def __sub__(self, other):
        new_list = TrialList()
        other_sentences = set()
        for value in other.trials.values():
            other_sentences.add(value['full_sentence'])

        for key, value in self.trials.items():
            if value['full_sentence'] not in other_sentences:
                new_list.trials[key] = value

        return new_list


def main():
    # trial_list = TrialList('mazesentences/data/generated_trials/trials_v121.1.json')
    # trial_list.add_file('mazesentences/data/generated_trials/trials_v101.8-final_version.json')
    # trial_list.add_file('mazesentences/data/generated_trials/trials_v131.1.json')
    # # trial_list.renumber_sentences()
    # trial_list.write_file('mazesentences/data/generated_trials/trials_v140.json')
    # trial_list.check_duplicates()
    list1 = TrialList('mazesentences/data/generated_trials/trials_total.1.json')
    list1.check_duplicates()
    list2 = TrialList('mazesentences/data/generated_trials/trials_v102.json')

    subtracted = list1 - list2
    subtracted.write_file('mazesentences/data/generated_trials/trials_subtracted.json')
    # subtracted.check_duplicates()

    # for key in list1.trials:
    #     print('Sentence #{}: {}'.format(key, (list1.trials[key] == list2.trials[key])))

if __name__ == '__main__':
    main()
