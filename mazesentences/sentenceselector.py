import requests
import json
from decimal import *
import unicodedata
import time
import random
from operator import itemgetter, attrgetter

from .elasticconnector import ElasticConnector

class Selector(ElasticConnector):
    def __init__(self, database_url, language, version, min_year=None):

        ElasticConnector.__init__(self, database_url, language, version, min_year=min_year)

    # def generate_choices(self, word, distractors, max_sentences=None):
    #     options = self._get_sentences(word, max_sentences)
    #
    #     for sentence in options:
    #         toks = sentence[1]
    #         print(toks)
    #         print()
    #         word_index = toks.index(word)
    #
    #         for tok in ([word] + distractors):
    #
    #             print(tok)
    #             for i in range(0, 5):
    #                 count = self._get_frame(toks[word_index-i:word_index] + [tok])
    #                 print('Ngram: {}\tCount: {}\tFrame: {}'.format(i+1, count, toks[word_index-i:word_index] + [tok]))
    #
    #             print()
    #
    # def _get_frame(self, context):
    #     terms = [{'term': {'n': len(context)}}]
    #
    #     for ix, word in enumerate(context):
    #         terms.append(
    #             {'term': {'token_{}'.format(ix + 1): '{}'.format(word)}}
    #         )
    #
    #     query_template = {
    #         'query': {
    #             'constant_score': {
    #                 'filter': {
    #                     'bool': {
    #                         'must': [
    #                             terms
    #                         ]
    #                     }
    #                 }
    #             }
    #         }
    #     }
    #
    #     query = json.dumps(query_template, ensure_ascii=False)
    #     # print(query)
    #     query = bytearray(query, 'utf-8')
    #     resp = requests.get('{}/_search'.format(self.ngram_index), data=query)
    #
    #     if resp.json()['hits']['total'] > 0:
    #         count = resp.json()['hits']['hits'][0]['_source']['total_count']
    #     else:
    #         count = 0
    #
    #     return int(count)

    def get_sentences(self, word, max_sentences=None):
        query = {
            "size": 5000,
            "query": {
                "function_score": {
                    "filter": {
                        "bool": {
                            "must": [
                                {"term": {"sentence.icu": "{}".format(word)}},
                                {"script": { "script": "doc['sentence.icu'].size() <= 20"}},
                                {"script": { "script": "doc['sentence'].size() > 4"}}
                            ]
                        }
                    },
                    "functions": [
                        {
                            "script_score": {
                                "script": "doc['sentence'].size() * {} % 1000003".format(random.randint(10, 100000000))
                            }
                        }
                    ]
                }
            }
        }

        query = json.dumps(query, ensure_ascii=False)
        # print(query)

        hits = None
        for i in range(5):
            try:
                resp = requests.post(
                    '{}/sentences/sentence/_search'.format(self.database_url),
                    data=bytearray(query, 'utf-8')
                )
                hits = resp.json()['hits']['hits']
                break
            except:
                time.sleep(1)
                pass

        sentences = []
        for hit in hits:
            sentence = hit['_source']['sentence']
            sentences.append(sentence)

        if len(sentences) == 0:
            return None

        print('Got {} sentences for {}...'.format(len(sentences), word))

        # return sentences
        return self._process_sentences(sentences, word, max_sentences)

    def _process_sentences(self, sentences, word, max_sentences):
        rejected = 0
        prohibited_chars = set([
            '？', '?', '“', '”',
            '(', ')', '[', ']',
            '}', '{', '〈', '〉', '…',
            '《', '》', '（', '）', '·'
        ])
        sentence_info = []
        process_start = time.perf_counter()
        for sentence in sentences:
            if (len(sentence_info) + rejected) % 100 == 0:
                print('Processed {}/{} sentences for {} ({} rejected) in {:.2f}s...'.format((len(sentence_info)+rejected), len(sentences), word, rejected, (time.perf_counter()-process_start)))
                # print(sentence_info[len(sentence_info)-10:])

            try:
                resp = requests.get(
                    '{}/{}/_analyze'.format(self.database_url, self.index),
                    data=bytearray(sentence, 'utf-8'),
                    params={'tokenizer': 'icu_tokenizer'}
                )
                tokens = resp.json()['tokens']
            except:
                rejected += 1
                continue
            quick_tokenized = []
            prev_end = 0
            for token in tokens:
                # if token['start_offset'] != prev_end:
                #     # actual_token = sentence[token['start_offset']:token['end_offset']]
                #     missing_token = sentence[prev_end:token['start_offset']]
                #     quick_tokenized.append(missing_token)

                quick_tokenized.append(token['token'])
                # prev_end = token['end_offset']

            if len(set(list(sentence)) & prohibited_chars) > 0:
                # print(set(list(sentence)))
                rejected += 1
                continue

            if len(quick_tokenized) < 5:
                rejected += 1
                # print('Too short: {}'.format(sentence))
                continue

            if len(quick_tokenized) > 20:
                rejected += 1
                # print('Too long: {}'.format(sentence))
                continue

            if word not in quick_tokenized:
                rejected += 1
                # print('Bad tokenization: {}'.format(quick_tokenized))
                continue

            if quick_tokenized.count(word) > 1:
                rejected += 1
                # print('Too many occurrences: {}'.format(sentence))
                continue

            if quick_tokenized.index(word) < 3:
                rejected += 1
                # print('Too early: {}'.format(sentence))
                continue

            if quick_tokenized.index(word) > len(quick_tokenized) * .85:
                rejected += 1
                # print('Too late: {}'.format(sentence))
                continue

            tokenized = self.tokenize_sentence(sentence)
            revised_tokenized = []
            for token in tokenized:
                if type(token) is str:
                    revised_tokenized.append(token)
                elif type(token) is tuple:
                    revised_tokenized += token

            tokenized = revised_tokenized

            num_words = len(tokenized)
            trans_freqs = self._transition_frequencies(tokenized)
            weighted_score = int(round(Decimal(trans_freqs/num_words)))

            old_weight = weighted_score
            punc_count = 0
            for char in list(sentence[:-1]):
                if unicodedata.category(char)[0] == 'P':
                    punc_count += 1
                    weighted_score *= (1 - (punc_count / len(sentence)))**punc_count

            weighted_score = int(round(weighted_score))

            # print(sentence)
            # print(punc_count, old_weight, weighted_score)
            # print()
            sentence_info.append([sentence, tokenized, num_words, trans_freqs, weighted_score])

            if max_sentences and len(sentence_info) > max_sentences:
                break

        sentence_info = sorted(sentence_info, key=itemgetter(3), reverse=True)
        # sentence_info = sorted(sentence_info, key=itemgetter(2), reverse=True)

        # for sentence in sentence_info[:100]:
        #     print(sentence)
        return sentence_info[:5]#, len(sentence_info), rejected

    def _transition_frequencies(self, sentence):
        pairs = len(sentence) - 1
        triplets = pairs - 1
        if pairs < 1:
            return -1

        indiv_freq_sum = 0
        pair_freq_sum = 0
        triplet_freq_sum = 0

        start_time = time.perf_counter()
        indiv_queries = []
        for word in sentence[:-1]:
            indiv_query = {
                'query': {
                    'constant_score': {
                        'filter': {
                            'bool': {
                                'must': [
                                    {'term': {'n': 1}},
                                    {'term': {'token_1': '{}'.format(word)}}
                                ]
                            }
                        }
                    }
                }
            }

            indiv_queries.append(json.dumps({}, ensure_ascii=False))
            indiv_queries.append(json.dumps(indiv_query, ensure_ascii=False))

        query = bytearray('\n'.join(indiv_queries) + '\n', 'utf-8')
        resp = requests.get('{}/_msearch'.format(self.ngram_index), data=query)

        for hit in resp.json()['responses']:
            hits = hit['hits']
            if hits['total'] > 0:
                indiv_count = hits['hits'][0]['_source']['total_count']
            else:
                indiv_count = 0

            indiv_freq_sum += indiv_count

        # print('Indiv freq time: {:.1f}'.format(time.perf_counter() - start_time))

        w1 = None
        w2 = None

        start_time = time.perf_counter()
        pair_queries = []
        for word in sentence:
            if not w2:
                w2 = word
                continue

            w1 = w2
            w2 = word

            pair_query = {
                'query': {
                    'constant_score': {
                        'filter': {
                            'bool': {
                                'must': [
                                    {'term': {'n': 2}},
                                    {'term': {'token_1': '{}'.format(w1)}},
                                    {'term': {'token_2': '{}'.format(w2)}}
                                ]
                            }
                        }
                    }
                }
            }
            # pair_query = bytearray(json.dumps(pair_query, ensure_ascii=False), 'utf-8')

            pair_queries.append(json.dumps({}, ensure_ascii=False))
            pair_queries.append(json.dumps(pair_query, ensure_ascii=False))

        query = bytearray('\n'.join(pair_queries) + '\n', 'utf-8')
        resp = requests.get('{}/_msearch'.format(self.ngram_index), data=query)
        # print(resp.json())

        for hit in resp.json()['responses']:
            # print(hit)
            # print()
            hits = hit['hits']
            if hits['total'] > 0:
                pair_count = hits['hits'][0]['_source']['total_count']
            else:
                pair_count = 0

            pair_freq_sum += pair_count

        # print('Pair freq time: {:.1f}'.format(time.perf_counter() - start_time))

        w1 = None
        w2 = None
        w3 = None

        start_time = time.perf_counter()
        triplet_queries = []
        for word in sentence:
            if not w3:
                w3 = word
                continue
            if not w2:
                w2 = w3
                w3 = word
                continue

            w1 = w2
            w2 = w3
            w3 = word

            triplet_query = {
                'query': {
                    'constant_score': {
                        'filter': {
                            'bool': {
                                'must': [
                                    {'term': {'n': 3}},
                                    {'term': {'token_1': '{}'.format(w1)}},
                                    {'term': {'token_2': '{}'.format(w2)}},
                                    {'term': {'token_3': '{}'.format(w3)}}
                                ]
                            }
                        }
                    }
                }
            }
            # triplet_query = bytearray(json.dumps(triplet_query, ensure_ascii=False), 'utf-8')
            triplet_queries.append(json.dumps({}, ensure_ascii=False))
            triplet_queries.append(json.dumps(triplet_query, ensure_ascii=False))

        query = bytearray('\n'.join(triplet_queries) + '\n', 'utf-8')
        resp = requests.get('{}/_msearch'.format(self.ngram_index), data=query)
        # print(resp.json())

        for hit in resp.json()['responses']:
            # print(hit)
            # print()
            hits = hit['hits']
            if hits['total'] > 0:
                triplet_count = hits['hits'][0]['_source']['total_count']
            else:
                triplet_count = 0

            triplet_freq_sum += triplet_count

        # print('Triple freq time: {:.1f}'.format(time.perf_counter() - start_time))

        smooth_avg = Decimal(indiv_freq_sum/(len(sentence) - 1))
        smooth_avg = (Decimal(0.50) * Decimal(pair_freq_sum/pairs)) + (Decimal(1-0.50) * smooth_avg)
        smooth_avg = (Decimal(0.50) * Decimal(triplet_freq_sum/triplets)) + (Decimal(1-0.50) * smooth_avg)

        return int(round(smooth_avg/Decimal(len(sentence)**1.5)))
