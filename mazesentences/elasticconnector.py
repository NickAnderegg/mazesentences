import requests
import simplejson as json
from operator import itemgetter
from decimal import *
from collections import namedtuple
import unicodedata
import time
import random
import yaml
from math import log10
random.seed(int(time.perf_counter()))

class ElasticConnectorBase(object):
    def __init__(self):
        self.languages = {
            'chinese_simplified': 'chi-sim-all',
            'english': 'eng-all',
            'english_million': 'eng-1M',
            'english_american': 'eng-US-all',
            'english_british': 'eng-gb-all',
            'english_fiction': 'eng-fiction-all'
        }

        self.versions = {
            2: '20120701',
            1: '20090715'
        }

        self.speech_tags = (
            'NOUN',   'VERB',   'ADJ',  'ADV',  'ADP',
            'PRON',   'PRT',    'DET',  'CONJ', 'NUM',
            'URL', 'START', 'END', 'X', 'ROOT', '.', '_'
        )

        self.punctuation = (
            '.', ',', ';', ':', '?', '!', "'", '"', '*', '(', ')', '&'
        )

class ElasticConnector(ElasticConnectorBase):
    def __init__(self, database_url, language, version, min_year=None):
        ElasticConnectorBase.__init__(self)

        # Set elasticsearch url
        if database_url[-1] == '/':
            database_url = database_url[:-1]
        self.database_url = database_url

        # Set language variable
        if language in self.languages.keys():
            self.language = self.languages[language]
        elif language in self.languages.values():
            self.language = language

        # Set version variable
        if version in {1,2}:
            self.version = self.versions[version]
        elif version in self.versions.values():
            self.version = version

        self.min_year = False if min_year is None else min_year

        self.index = 'ngrams-{}-{}-ngrams'.format(self.language, self.version)
        self.ngram_index = '{}/{}/ngram'.format(self.database_url, self.index)

        self.total_index = '{}/ngrams-{}-{}-sources/total'.format(self.database_url, self.language, self.version, self.index)

        totals_query = { 'query': { 'range': { 'year': { 'gte': self.min_year }}}}

        totals_query = bytearray(json.dumps(totals_query, ensure_ascii=False), 'utf-8')
        resp = requests.get('{}/_search'.format(self.total_index), data=totals_query)

        tots = resp.json()['hits']['hits']

        self.totals = {'counts': Decimal(0), 'volumes': Decimal(0), 'pages': Decimal(0)}
        for total in tots:
            self.totals['counts'] += total['_source']['counts']
            self.totals['volumes'] += total['_source']['volumes']
            self.totals['pages'] += total['_source']['pages']

        getcontext().prec = 12

    def ngrams_count(self):
        resp = requests.get('{}/_count'.format(self.ngram_index))
        if resp.status_code == requests.codes.ok:
            return resp.json()['count']
        else:
            return False

    def distractor_sentence(self, sentence, critical_token):
        ignore_parts = {'URL', 'START', 'END', 'ROOT', '.', 'X', 'NUM', '_'}
        ignore_low = {'PRON', 'CONJ'}

        # print('Target sentence: {}'.format(sentence))
        tokenized = self.tokenize_sentence(sentence)
        # print('Tokenized:\t{}'.format(tokenized))

        multi_parts = []
        revised_tokenized = []
        count = 0
        for token in tokenized:
            if type(token) is str:
                revised_tokenized.append(token)
                multi_parts.append(count)
                count += 1
            elif type(token) is tuple:
                multi_parts += [count]*len(token)
                revised_tokenized += token
                count += len(token)

        tokenized = revised_tokenized

        if len(tokenized[-1]) > 1 or unicodedata.category(tokenized[-1])[0] != 'P':
            tokenized.append('.')
            multi_parts.append(len(multi_parts))

        # print('De-chunked:\t{}'.format(tokenized))
        # print(multi_parts)
        # print(tokenized)

        if critical_token not in tokenized:
            print('Critical token does not appear as a standalone character')
            return False

        antisentence = ['Ｘ'*len(tokenized[0])]
        for i in range(1, len(tokenized)):
            if tokenized[i] == critical_token:
                antisentence.append('＃'*len(tokenized[i]))
                continue
            elif len(tokenized[i]) == 1 and unicodedata.category(tokenized[i])[0] == 'P':
                antisentence.append('*')
                continue
            elif multi_parts[i] != i:
                continue
            combined_probs = dict()
            word_probs = dict()
            # ProbTuple = namedtuple('Probabilities', [2, 3, 4, 5])
            for j in range(1, min(i+1, 5)):
                n = j+1
                search_tuple = []
                for slot in range(j, 0, -1):
                    search_item = tokenized[i-slot]
                    search_tuple.append(search_item)
                search_tuple.append(None)
                search_tuple = tuple(search_tuple)
                # print(tokenized[i], n, tuple(search_tuple))

                slot_probabilities = self.get_slot_probabilities(n, n, search_tuple)
                if slot_probabilities is False:
                    continue

                for prob in slot_probabilities['combined']:
                    if prob[1] in (ignore_parts | ignore_low):
                        continue
                    if prob[1] not in combined_probs:
                        combined_probs[prob[1]] = dict.fromkeys([2,3,4,5], 0)
                    combined_probs[prob[1]][n] += prob[3]

                for prob in slot_probabilities['words']:
                    if prob[0] not in word_probs:
                        word_probs[prob[0]] =  dict.fromkeys([2,3,4,5], 0)
                    word_probs[prob[0]][n] += prob[2]

            # print(tokenized[i])
            weighted_combined = dict()
            for key, probs in combined_probs.items():
                smooth_avg = probs[2]
                for k in range(3, 6):
                    if probs[k] is 0:
                        break
                    smooth_avg = (Decimal(0.67) * probs[k]) + (Decimal(1 - 0.67) * smooth_avg)
                weighted_combined[key] = smooth_avg

            weighted_combined = list(weighted_combined.items())
            weighted_combined = sorted(weighted_combined, key=itemgetter(1))
            # print(weighted_combined)
            # print()

            weighted_words = dict()
            for key, probs in word_probs.items():
                smooth_avg = probs[2]
                for k in range(3, 6):
                    if probs[k] is 0:
                        break
                    smooth_avg = (Decimal(0.67) * probs[k]) + (Decimal(1 - 0.67) * smooth_avg)
                weighted_words[key] = smooth_avg

            weighted_words = list(weighted_words.items())
            weighted_words = sorted(weighted_words, key=itemgetter(1))
            # print({key for key, val in weighted_words})
            # print(weighted_words[-10:])
            # freq_cutoff = weighted_words[-10][1]/1000
            # # print(freq_cutoff)
            # below, above = 0, 0
            # for key, val in weighted_words:
            #     if val < freq_cutoff:
            #         below += 1
            #     else:
            #         above += 1
            # print(below, above)

            # print(tokenized[i])
            prohibited_parts = set()
            cutoff_parts = set()

            if len(weighted_combined) > 0:
                cutoff = round(weighted_combined[int(len(weighted_combined)/5*3)][1])
                # print('Cutoff: ', cutoff)
                for part, freq in weighted_combined:

                    if freq >= Decimal(100/8):
                        # prohibited_parts.append(
                        #     {'regexp': {'token_1': '.*_{}'.format(part)}}
                        # )
                        # print('Prohibited Part: ', part, '\tFreq: ', freq)
                        prohibited_parts.add(part)
                    elif freq >= cutoff:
                        # print('Cutoff Part: ', part, '\tFreq: ', freq)
                        cutoff_parts.add(part)
                    # else:
                        # print('Allowed Part: ', part, '\tFreq: ', freq)

            # for part in (ignore_parts | ignore_low):
            #     # prohibited_parts.append(
            #     #     {'regexp': {'token_1': '.*_{}'.format(part)}}
            #     # )
            #     prohibited_parts.add(part)

            # print(prohibited_parts)

            # print(prohibited_parts)

            token_length = len(tokenized[i])
            # if i+1 < len(multi_parts) and multi_parts[i+1] != i+1:
            for ix in range(i+1, len(multi_parts)):
                # print(multi_parts[ix], ix)
                if multi_parts[ix] != ix:
                    token_length += len(tokenized[ix])
                else:
                    break
            # print(token_length)

            # prohibited_items = []
            prohibited_items = set()
            if len(weighted_words) > 5:
                freq_cutoff = weighted_words[-5][1]/1000
            else:
                freq_cutoff = 0
            for word, freq in reversed(weighted_words):
                # if len(prohibited_items) >= 512:
                #     break
                if len(word) != token_length:
                    continue
                if freq >= freq_cutoff:
                    # prohibited_items.append(
                    #     { 'regexp': {'token_1': '{}_.*'.format(word)}}
                    # )
                    prohibited_items.add(word)


                # for ix, parent in enumerate(multi_parts[i+1:]):
                #     if parent != i+ix

            # prohibition = prohibited_parts + prohibited_items[:256]
            possibilities_query = {
                'size': 2500,
                'query': {
                    'function_score': {
                        'filter': {
                            'bool': {
                                'must': [
                                    {'term': {'n': 1}},
                                    {'regexp': {'token_1': '[一-鿌]{{{}}}_.*'.format(token_length)}}
                                ],
                                'must_not': [
                                    {'regexp': {'token_1': '.*_{}'.format(part)}} for part in prohibited_parts
                                ]

                                #,
                                #'must_not': prohibition#prohibited_items[:512] + prohibited_parts
                            }
                        },
                        'functions': [
                            {
                                'script_score': {
                                    'script': "doc['total_count'].value * {} % 1000003".format(random.randint(10, 100000000))
                                }
                            }
                        ]
                    }
                }
            }

            # print(json.dumps(possibilities_query, indent=2, ensure_ascii=False))

            possibilities_query = bytearray(json.dumps(possibilities_query, ensure_ascii=False), 'utf-8')
            resp = requests.get(
                '{}/_search'.format(self.ngram_index),
                data=possibilities_query
            )
            if resp.status_code == requests.codes.ok:
                possibilities = resp.json()['hits']['hits']
                # print(resp.json()['hits']['total'])
                # print('Took:', resp.json()['took'])
            else:
                print(resp.text)
                print(resp.request.body.decode('utf-8'))

            # print(json.dumps(possibilities[:2], indent=2, ensure_ascii=False))

            sort_poss = []
            for result in possibilities:
                orig_sort = round(log10(result['_source']['total_count']), 3)
                new_sort = orig_sort - abs(random.gauss(1, .25))
                # print(orig_sort, new_sort)
                sort_poss.append((new_sort, result))

            possibilities = [result for sort, result in sorted(sort_poss, key=itemgetter(0), reverse=True)]

            for result in possibilities:
                poss_token, poss_part = tuple(result['_source']['token_1'].split('_'))

                if poss_token in tokenized:
                    continue
                elif poss_token in antisentence:
                    continue
                elif poss_token in prohibited_items:
                    continue
                elif poss_part in (ignore_parts | ignore_low):
                    continue
                elif poss_part in prohibited_parts:
                    continue
                elif i < 3 and poss_part == 'PRT':
                    continue

                pos_dist = self.token_pos_dist(poss_token)
                if not pos_dist:
                    continue
                reject = False
                for pos, freq in pos_dist:
                    if freq > Decimal(1):
                        if pos in prohibited_parts:
                            # print('Rejecting prohibited {} ({}), {} / Freq: {}'.format(poss_token, poss_part, pos, freq))
                            reject = True
                            break
                        elif pos in cutoff_parts:
                            if freq > Decimal(20) or (freq > Decimal(5) and i < 3):
                                # print('Rejecting cutoff {} ({}), {} / Freq: {}'.format(poss_token, poss_part, pos, freq))
                                reject = True
                                break

                if reject:
                    continue
                    # if pos in prohibited_parts:
                    #     print('Token: {}\tPOS: {}\tFreq: {}'.format(poss_token, pos, freq))

                # print('Accepting token: {}\t POS: {}\n'.format(poss_token, poss_part))

                # poss_token = poss_token.split('_')
                # antisentence.append(poss_token[0])
                antisentence.append(poss_token)
                break

        # print('Anti-sentence:\t{}'.format(antisentence))
        final_tokenized = []
        for ix, token in enumerate(tokenized):
            if ix == multi_parts[ix]:
                final_tokenized.append(token)
            else:
                final_tokenized[-1] += token


        tokenized = final_tokenized

        paired_sentence = []
        for i in range(len(tokenized)):
            # if antisentence[i] == '*':
            #     paired_sentence[-1] = [paired_sentence[-1][0] + tokenized[i], paired_sentence[-1][1] + tokenized[i]]
            # else:
            try:
                paired_sentence.append([tokenized[i], antisentence[i]])
            except IndexError:
                print('Error! Antisentence generated incorrectly!')
                print('Sentence:\t', tokenized)
                print('Antisent:\t', antisentence)
                return 'Error! {}'.format(paired_sentence)

        # print(json.dumps(paired_sentence, encoding='utf-8', ensure_ascii=False))
        return paired_sentence

    def tokenize_sentence(self, sentence):

        # manual_tokens = []
        #
        # if '<' in sentence:
        #     start_ix = 0
        #     while start_ix < len(sentence) and start_ix != -1:
        #         manual_tok_start = sentence.find('<', start_ix)
        #         manual_tok_end = sentence.find('>', start_ix)
        #
        #         if manual_tok_start < manual_tok_end and manual_tok_start != -1:
        #             manual_tokens.append((
        #                 sentence[manual_tok_start + 1 : manual_tok_end],
        #                 manual_tok_start + 1,
        #                 manual_tok_end - (manual_tok_start+1)
        #             ))
        #         else:
        #             start_ix = -1
        #             break
        #
        #         start_ix = manual_tok_end + 1
        #
        #     print('Manual tokens marked:', sentence)
        #     print('Manual tokens: ', manual_tokens)
        #     sentence = sentence.replace('<', '').replace('>', '')
        #
        # print('To be tokenized: ', sentence)

        resp = requests.get(
            '{}/{}/_analyze'.format(self.database_url, self.index),
            data=bytearray(sentence, 'utf-8'),
            params={'tokenizer': 'icu_tokenizer'}
        )
        tokens = resp.json()['tokens']
        tokenized = []
        # retokenized = []
        prev_end = 0
        for token in tokens:
            # if token['token'] == ',':
            if token['start_offset'] != prev_end:
                # actual_token = sentence[token['start_offset']:token['end_offset']]
                missing_token = sentence[prev_end:token['start_offset']]
                # if ord(actual_token) < 128:
                    # print(token['token'], actual_token)
                    # token['token'] = actual_token
                if len(missing_token) == 1 and ord(missing_token) > 128:
                    char_name = unicodedata.name(missing_token)
                    normalized_start = char_name.find(' ') + 1
                    normalized = char_name[normalized_start:]
                    # print(token['token'], actual_token, unicodedata.lookup(normalized))
                    # token['token'] = unicodedata.lookup(normalized)
                    try:
                        missing_token = unicodedata.lookup(normalized)
                    except KeyError:
                        pass

                tokenized.append(missing_token)

            if len(token['token']) == 1:
                tokenized.append(token['token'])
            elif self.check_token_exists(token['token']):
                tokenized.append(token['token'])
            # elif len(token['token']) == 1 and unicodedata.category(token['token'])[0] == 'P':
            #     tokenized.append(token['token'])
            # elif len(token['token']) == 1:
            #     tokenized.append(toke)
            else:
                token_split = list(token['token'])
                valid_tokens = []
                for char in token_split:
                    if self.check_token_exists(char):
                        valid_tokens.append(char)

                if len(token_split) == len(valid_tokens):
                    # retokenized += token_split
                    tokenized.append(tuple(token_split))
                else:
                    tokenized.append(token['token'])
                    # retokenized.append(token['token'])

                # print(token['token'], token_split)

            prev_end = token['end_offset']

        # if len(manual_tokens) > 0:
        #     for tok in manual_tokens:
        #         if tok[0] in tokenized:
        #             continue
        #         else:
        #             current_tok = 0
        #             char_count = 0
        #             while current_tok < len(manual_tokens):
        #                 if len(manual_tokens[current_tok]) == 1:
        #                     if manual_tokens[current_tok] in tok[0]:


        # print('Tokenized: ', tokenized)
        # quit()
        return tokenized

    def token_pos_dist(self, token, as_dict=False):
        token_check_query = {
            'query': {
                'constant_score': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'n': 1}},
                                {'wildcard': {'token_1': '{}_*'.format(token)}}
                            ]
                        }
                    }
                }
            }
        }
        token_check_query = bytearray(json.dumps(token_check_query, ensure_ascii=False), 'utf-8')
        token_check = requests.get(
            '{}/_search'.format(self.ngram_index),
            data=token_check_query
        )
        if token_check.status_code == requests.codes.ok:
            hits = token_check.json()['hits']
            parts = hits['hits']
            total_found = hits['total']
        else:
            print(token_check.text)
            print(token_check.request.body.decode('utf-8'))
            return False

        if not total_found:
            # print(token_check.text)
            return False

        count_sum = 0
        dist_raw = dict()
        for result in parts:
            tok, speech_part = tuple(result['_source']['token_1'].split('_'))
            total_count = int(result['_source']['total_count'])

            count_sum += total_count

            dist_raw[speech_part] = total_count

        # print(dist_raw)
        dist = []
        for key, value in dist_raw.items():
            dist.append((key, value / count_sum * 100))

        dist = sorted(dist, key=itemgetter(1), reverse=True)

        return dist

    def check_token_exists(self, token):
        # token_check_query = {'query': {'constant_score': {'filter': {'bool': {'must': [{'term': {'n': 1}}, {'regexp': {'token_1': '{}(_.*)?'.format(token)}}]}}}}}
        token_check_query = {'query': {'constant_score': {'filter': {'bool': {'must': [{'term': {'n': 1}}, {'term': {'token_1': '{}'.format(token)}}]}}}}}
        token_check_query = bytearray(json.dumps(token_check_query, ensure_ascii=False), 'utf-8')
        token_check = requests.get(
            '{}/_count'.format(self.ngram_index),
            data=token_check_query
        )
        if token_check.status_code != requests.codes.ok:
            raise RuntimeError('Could not connect to database')

        token_check = token_check.json()['count']

        if token_check > 0:
            return True
        else:
            return False



    def get_contexts(self, n, search_slot, target_token):

        match_fields = [{ 'term': { 'n': n }}, { 'term': { ('token_'+str(search_slot)): target_token}}]

        context_query = {
            'query': { 'constant_score': { 'filter': { 'bool': { 'must': match_fields }}}}
        }

        count_query = bytearray(json.dumps(context_query, ensure_ascii=False), 'utf-8')
        resp = requests.get('{}/_count'.format(self.ngram_index), data=count_query)
        resp_count = resp.json()['count']
        if resp_count == 0:
            return False

        # print(resp_count)

        contexts = []
        contexts_sum = 0
        context_query['size'] = 2500
        context_query = bytearray(json.dumps(context_query, ensure_ascii=False), 'utf-8')
        resp = requests.get('{}/_search'.format(self.ngram_index), data=context_query, params={'scroll':'1m'})
        if resp.status_code != requests.codes.ok:
            print('Failure')
            print(resp.request.body.decode('utf-8'))
            return False

        processing_time = resp.json()['took']
        scroll_id = resp.json()['_scroll_id']
        for hit in resp.json()['hits']['hits']:
            contexts.append(hit['_source'])
            contexts_sum += hit['_source']['total_count']

        print('Number of contexts:', resp_count)

        # while len(contexts) < resp_count:
        #     scroll_query = bytearray(json.dumps({'scroll': '1m', 'scroll_id': scroll_id}), 'utf-8')
        #     resp = requests.get('{}/_search/scroll'.format(self.database_url), data=scroll_query).json()
        #     # if resp.status_code != requests.codes.ok:
        #     #     print('Failure')
        #     #     print(resp.request.body.decode('utf-8'))
        #     #     return False
        #
        #     processing_time += resp['took']
        #     hits = resp['hits']['hits']
        #
        #     for hit in hits:
        #         contexts.append(hit['_source'])
        #         contexts_sum += hit['_source']['total_count']
        #
        #         if len(contexts) % 5000 == 0:
        #             print('{} contexts in {}'.format(len(contexts), processing_time))

        processed_contexts = []
        for context in contexts:
            context_data = []
            for i in range(1, n+1):
                if i == search_slot:
                    if context['token_'+str(i)] == target_token:
                        context_data.append('_NGRAMTARGET_')
                    else:
                        print('Error:', context['token_'+str(i)])
                        break
                else:
                    token = context['token_'+str(i)]
                    if token.count('_') == 1:
                        token, tag = tuple(token.split('_'))
                    elif token.startswith('_') and token.endswith('_'):
                        token, tag = None, token
                    else:
                        tag = None

                    context_data.append((token, tag))

            context_data.append(context['total_count'])
            context_data.append(Decimal(context['total_count']) / contexts_sum * 100)

            if len(context_data) != n+2:
                continue

            processed_contexts.append(tuple(context_data))
            # if len(processed_contexts) % 25 == 0:

        return processed_contexts

    def get_slot_probabilities(self, n, search_slot, tokens):
        context = dict()

        if type(tokens) is tuple:
            if len(tokens) is not n:
                raise IndexError('Size of tokens tuple not equivalent to n')
            elif tokens.count(None) != 1:
                raise ValueError('Tokens tuple must have exactly one empty slot')
            else:
                for i, token in enumerate(tokens):
                    if token is not None:
                        context['token_' + str(i+1)] = token
        elif type(tokens) is dict:
            if len(tokens) != n - 1:
                raise IndexError('Size of tokens dict not equivalent to n')
            for key in tokens.keys():
                if key < 1 or key > n:
                    raise KeyError('Tokens dict keys are invalid')
                else:
                    if tokens[key] is None:
                        raise ValueError('Tokens dict must not contain an empty slot')
                    context['token_' + str(key)] = tokens[key]

        match_fields = [{ 'term': { 'n': n }}]
        for key, value in context.items():
            if (len(value) == 1
            and unicodedata.category(value)[0] == 'P'
            and value != '.'):
                match_fields.append({ 'regexp': { key: '{}(_{{1}}.*)?'.format(value)}})
            else:
                match_fields.append({ 'term': { key: value}})

        context_query = {
            'from': 0,
            # 'sort': {'total_count': {'order': 'desc'} },
            'query': {
                'constant_score': {
                    'filter': {
                        'bool': {
                            'must': match_fields
                        }
                    }
                }
            }
        }

        count_query = bytearray(json.dumps(context_query, ensure_ascii=False), 'utf-8')
        resp = requests.get('{}/_count'.format(self.ngram_index), data=count_query)
        # print(resp.request.body.decode('utf-8'))
        resp_count = resp.json()['count']
        if resp_count == 0:
            return False

        context_query['size'] = 10000

        context_query = bytearray(json.dumps(context_query, ensure_ascii=False), 'utf-8')
        resp = requests.get('{}/_search'.format(self.ngram_index), data=context_query)
        if resp.status_code != requests.codes.ok:
            print('Failure')
            print(resp.text)
            return False

        hits = resp.json()['hits']
        hits_total = hits['total']
        if hits_total != resp_count:
            raise RuntimeError('Error with data return from database: counts do not match')
        # print(hits_total)
        hits = hits['hits']

        # print(json.dumps(hits, ensure_ascii=False, indent=2))

        # print(resp.request.body.decode('utf-8'))

        context_possibilities = {
            'words': [],
            'parts': [],
            'combined': [],
            'punctuation': []
        }

        context_totals = {
            'words': 0,
            'parts': 0,
            'combined': 0,
            'punctuation': 0
        }
        # additive = {}
        search_tok = 'token_' + str(search_slot)
        for hit in hits:
            hit = hit['_source']

            for i in range(1, n+1):
                if i is search_slot:
                    continue
                elif hit['token_' + str(i)] != context['token_' + str(i)]:
                    if not (unicodedata.category(context['token_' + str(i)])[0] == 'P'
                    and context['token_' + str(i)] == hit['token_' + str(i)][0]):
                        print('Error!')

            if hit[search_tok].startswith('_') and hit[search_tok].endswith('_'):
                if hit[search_tok][1:-1] in self.speech_tags:
                    context_possibilities['parts'].append((
                        hit[search_tok][1:-1],
                        hit['total_count']
                    ))
                    context_totals['parts'] += hit['total_count']
                elif hit[search_tok] == '_':
                    context_possibilities['parts'].append((
                        hit[search_tok],
                        hit['total_count']
                    ))
                    context_totals['parts'] += hit['total_count']
                else:
                    print('Not found in speech tags:', hit)
            elif hit[search_tok].count('_') is 1:
                token, tag = tuple(hit[search_tok].split('_'))
                context_possibilities['combined'].append((
                    token,
                    tag,
                    hit['total_count']
                ))
                context_totals['combined'] += hit['total_count']

            elif hit[search_tok] in self.punctuation:
                context_possibilities['punctuation'].append((
                    hit[search_tok],
                    hit['total_count']
                ))
                context_totals['punctuation'] += hit['total_count']
            else:
                context_possibilities['words'].append((
                    hit[search_tok],
                    hit['total_count']
                ))
                context_totals['words'] += hit['total_count']

        for key, context_list in context_possibilities.items():
            for i in range(len(context_list)):
                j = 2 if key == 'combined' else 1
                context_possibilities[key][i] += (
                    Decimal(context_possibilities[key][i][j]) / Decimal(context_totals[key]) * 100,
                )

        return context_possibilities
        # print(json.dumps(context_possibilities, ensure_ascii=False, indent=2))
