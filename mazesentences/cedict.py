import pathlib
import re

class CEDict(object):

    def __init__(self, file_name=None):

        self.file_name = file_name

    def load_dict(self, ignore_roman=False, dict_primary='simplified', load_both=True):
        entry = re.compile(r"^(?P<traditional>\w+)\s{1}(?P<simplified>\w+)\s{1}\[(?P<pinyin>.+)\]\s{1}/(?P<definitions>.+)")

        with pathlib.Path(self.file_name).open('r', encoding='utf-8') as f:
            self.dictionary = dict()

            self._simp_loaded = False
            self._trad_loaded = False
            if dict_primary == 'simplified' or load_both is True:
                self.simplified_set = set()
                self._simp_loaded = True
            if dict_primary == 'traditiona' or load_both is True:
                self.traditional_set = set()
                self._trad_loaded = True


            for line in f:
                if line[0] == '#':
                    continue

                match = entry.match(line)
                if match is not None:
                    headword = match.group(dict_primary)

                    if not all([False if ord(char) < 128 else True for char in headword]):
                        continue

                    if headword not in self.dictionary:
                        self.dictionary[headword] = dict()
                        if dict_primary == 'simplified' or load_both is True:
                            self.dictionary[headword]['simplified'] = match.group('simplified')
                            self.simplified_set.add(self.dictionary[headword]['simplified'])
                        if dict_primary == 'traditional' or load_both is True:
                            self.dictionary[headword]['traditional'] = match.group('traditional')
                            self.traditional_set.add(self.dictionary[headword]['traditional'])
                        self.dictionary[headword]['pinyin'] = match.group('pinyin').lower()
                        self.dictionary[headword]['definitions'] = match.group('definitions')[0:-1].split('/')

    def check_simp_word(self, word):
        if self._simp_loaded:
            if word in self.simplified_set:
                return True
            else:
                return False
        else:
            raise ValueError('Simplified dictionary was not loaded at object init')


if __name__ == '__main__':
    cedict = CEDict(file_name='data/cedict_1_0_ts_utf-8_mdbg.txt')
    cedict.load_dict(ignore_roman=True, load_both=False)
