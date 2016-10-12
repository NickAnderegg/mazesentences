import requests
import simplejson as json
from decimal import *

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
            'ADJ',    'ADP',    'ADV',    'CONJ',   'DET',
            'NOUN',   'NUM',    'PRON',   'PRT',    'VERB', '.'
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

    def tokenize_sentence(self, sentence):
        resp = requests.get(
            '{}/_analyze'.format(self.database_url),
            data=bytearray(sentence, 'utf-8'),
            params={'tokenizer': 'smartcn_tokenizer'}
        )
        tokens = resp.json()['tokens']
        tokenized = []
        for token in tokens:
            tokenized.append(token['token'])

        return tokenized

        # def ngram_query(n, tokens):
        #     if n == 1 and type(tokens) is not tuple:
        #         tokens = (tokens, )
        #     match_fields = [{ 'term': { 'n': n }}]
        #     for ix, token in enumerate(tokens):
        #         match_fields.append({ 'term': { ('token_'+str(ix+1)): token}})
        #
        #     return bytearray(json.dumps(
        #                 {
        #                     'size': 10000,
        #                     'query': { 'constant_score': { 'filter': { 'bool': { 'must': match_fields}}}}
        #                 },
        #             ensure_ascii=False
        #         ), 'utf-8'
        #     )

        # word_counts = [[0] * len(chars) for x in range(4)]
        # for i in range(len(chars)):
        #     for j in range(1, 5):
        #         if i+j > len(chars):
        #             continue
        #
        #         search_term = ''.join(chars[i:(i+j)])
        #
        #         resp = requests.get(
        #             '{}/_search'.format(self.ngram_index),
        #             data=ngram_query(1, search_term)
        #         )
        #
        #         resp = resp.json()
        #         if resp['hits']['total'] > 0:
        #             word_counts[j-1][i] = (resp['hits']['hits'][0]['_source']['total_count'], search_term)
        #         else:
        #             word_counts[j-1][i] = (0, search_term)
        #
        # for row in word_counts:
        #     print(row)
        #     print()
        # # return
        #
        # empty_list = [0 for x in range(len(chars))]
        # counts = [
        #     [[0] * len(chars) for x in range(4)],
        #     [[0] * len(chars) for x in range(4)],
        #     [[0] * len(chars) for x in range(4)],
        #     [[0] * len(chars) for x in range(4)]
        # ]
        # # print(counts)
        # for i in range(len(chars)):
        #     for j in range(1, 5):
        #         for k in range(1, 5):
        #             if i+j+k > len(chars):
        #                 continue
        #
        #             search_tuple = (
        #                 ''.join(chars[i:(i+j)]),
        #                 ''.join(chars[(i+j):(i+j+k)])
        #             )
        #
        #             resp = requests.get(
        #                 '{}/_search'.format(self.ngram_index),
        #                 data=ngram_query(2, search_tuple)
        #             )
        #             resp = resp.json()
        #             if resp['hits']['total'] > 0:
        #                 counts[j-1][k-1][i] = (resp['hits']['hits'][0]['_source']['total_count'], search_tuple)
        #             else:
        #                 counts[j-1][k-1][i] = (0, search_tuple)
        #
        #     # print(i+1, '/', len(chars))
        #
        # tokenized_sentence = []
        # chosen_token = None
        # chosen_bigram = None
        # i = 0
        # while i < len(chars):
        #     for j in range(3, -1, -1):
        #         if i+j >= len(chars):
        #             continue
        #
        #         if word_counts[j][i][0] > 0:
        #             if i > 0:
        #                 print(word_counts[j][i], chosen_bigram[1])
        #                 if word_counts[j][i][1] != chosen_bigram[1][1]:
        #                     continue
        #             chosen_token = word_counts[j][i]
        #
        #             for k in range(3, -1, -1):
        #                 if counts[j][k][i][0] > 0:
        #                     chosen_bigram = counts[j][k][i]
        #                     break
        #
        #             break
        #
        #     print(chosen_token, chosen_bigram)
        #
        #     tokenized_sentence.append(chosen_token)
        #     i += len(chosen_token)
        #
        # print(tokenized_sentence)


        # print(sentence)
        # for row in counts:
        #     for sub in row:
        #         print(sub)
        #     print()
        # print(sentence)
        # print(counts)

        # print(ngram_query(2, (''.join(chars[0:2]), chars[2])))

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
        context_query['size'] = 5000
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

        print('Number of contexts:', len(contexts))

        while len(contexts) < resp_count:
            scroll_query = bytearray(json.dumps({'scroll': '1m', 'scroll_id': scroll_id}), 'utf-8')
            resp = requests.get('{}/_search/scroll'.format(self.database_url), data=scroll_query).json()
            # if resp.status_code != requests.codes.ok:
            #     print('Failure')
            #     print(resp.request.body.decode('utf-8'))
            #     return False

            processing_time += resp['took']
            hits = resp['hits']['hits']

            for hit in hits:
                contexts.append(hit['_source'])
                contexts_sum += hit['_source']['total_count']

                if len(contexts) % 5000 == 0:
                    print('{} contexts in {}'.format(len(contexts), processing_time))

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
            match_fields.append({ 'term': { key: value}})

        context_query = {
            'from': 0,
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
        resp_count = resp.json()['count']
        if resp_count == 0:
            return False

        context_query['size'] = resp_count

        context_query = bytearray(json.dumps(context_query, ensure_ascii=False), 'utf-8')
        resp = requests.get('{}/_search'.format(self.ngram_index), data=context_query)
        if resp.status_code != requests.codes.ok:
            print('Failure')
            return False

        hits = resp.json()['hits']
        hits_total = hits['total']
        if hits_total != resp_count:
            raise RuntimeError('Error with data return from database: counts do not match')
        print(hits_total)
        hits = hits['hits']

        # print(json.dumps(hits, ensure_ascii=False, indent=2))

        print(resp.request.body.decode('utf-8'))

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
                    print('Error!')

            if hit[search_tok].startswith('_') and hit[search_tok].endswith('_'):
                if hit[search_tok][1:-1] in self.speech_tags:
                    context_possibilities['parts'].append((
                        hit[search_tok][1:-1],
                        hit['total_count']
                    ))
                    context_totals['parts'] += hit['total_count']
                else:
                    print('Not found in speech tags')
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
