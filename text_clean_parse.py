def load_dataset(filepath):
    '''Pulls data in from given path'''
    import pandas as pd 
    df = pd.read_csv(filepath)
    return df

def filter_tokens(sentence):
    ''' Function to remove non-english words, punctuation, and stopwords from tokens'''
    token_arr = []
    for w in sentence:
        if w in english_word_set:
            if w not in stopwords_ and w not in punctuation_:
                token_arr.append(w)
    return token_arr

def remove_accents(input_string):
    nfkd_form = unicodedata.normalize('NFKD', input_string)
    only_ascii = nfkd_form.encode('ASCII', 'ignore')
    return only_ascii.decode()

def tokenize_email_subject_line(df, col_name='MetadataSubject'):
    '''Ingests dataframe, a given column of string data, and returns
        an array of filtered, stemmed, tokenized words'''
    main_token_arr = []
    lemmatizer = WordNetLemmatizer()
    pattern1 = r'(h:)'
    pattern2 = r'(latest:)'
    for sent in df[col_name]:
        sent = str(sent)
        sent = sent.lower()
        sent = re.sub(pattern1, '', sent)
        sent = re.sub(pattern2, '', sent)
        sent = remove_accents(sent)
        sent = word_tokenize(sent)
        sent = filter_tokens(sent)
        lem_tokens = list(map(lemmatizer.lemmatize, sent))
        main_token_arr.append(lem_tokens)
    return main_token_arr

def create_english_wordset():
    '''Declare and update english wordset'''
    english_word_set = set(words.words())
    world_leaders = ['QADDAFI','El','Magariaf','Mohmmed', 'Morsi','BENGHAZI', 'Abclelhakim','Belhaj',
                 'Sufi','Salafist', 'Yussef','Obama','Mohammed','USG','Baghdad','Saif', 'al','Islam',
                 'RECEP','TAYYIP', 'ERDOGAN','POTUS','Myung','bak','Sarkozy','Merkel','Powell'
                    ]
    for n in world_leaders:
        n = n.lower()
        english_word_set.add(n)
    return english_word_set

def tokenize_email_raw_text_line(df, col_name='RawText'):
    '''Ingests dataframe, a given column of string data, and returns
        an array of filtered, stemmed, tokenized words'''
    main_token_arr = []
    lemmatizer = WordNetLemmatizer()
    raw_pat1 = r'(\\n)'
    raw_pat2 = r'(Case No..............)'
    raw_pat3 = r'(Doc No...........)'
    raw_pat4 = r'(Date............)'
    raw_pat5 = r'(SUBJECT TO AGREEMENT ON SENSITIVE INFORMATION & REDACTIONS)'
    raw_pat6 = r'(NO FOIA WAIVER)'
    for sent in df[col_name]:
        sent = str(sent)
        sent = sent.lower()
        sent = re.sub(raw_pat1, '', sent)
        sent = re.sub(raw_pat2, '', sent)
        sent = re.sub(raw_pat3, '', sent)
        sent = re.sub(raw_pat4, '', sent)
        sent = re.sub(raw_pat5, '', sent)
        sent = re.sub(raw_pat6, '', sent)
        sent = remove_accents(sent)
        sent = word_tokenize(sent)
        sent = filter_tokens(sent)
        lem_tokens = list(map(lemmatizer.lemmatize, sent))
        main_token_arr.append(lem_tokens)
    return main_token_arr

def corpus_creator(token_arr):
    '''explodes nested array into word corpus'''
    corpus = set()
    for arr in token_arr:
        for word in arr:
            corpus.add(word)
    return corpus

def clean_token_dataframe(raw_tokens, subjectline_tokens):
    index_nums = list(range(7945))
    cleaned_df = pd.DataFrame(columns=['Raw_text_tokens_cleaned', 'Subjectline_tokens'], index=index_nums)
    for i in range(7945):
        cleaned_df.loc[i] = [raw_text_tokens[i], subjectline_tokens[i]]
    return cleaned_df

if __name__ == '__main__':
    import pandas as pd
    import numpy as np
    import nltk
    import unicodedata
    from nltk.tokenize import sent_tokenize
    from nltk.tokenize import word_tokenize
    from nltk.corpus import stopwords
    nltk.download('stopwords')
    nltk.download('punkt')
    nltk.download('wordnet')
    nltk.download('words')
    nltk.download('brown')
    from nltk.corpus import words
    from nltk.corpus import brown
    import string
    from nltk.stem.porter import PorterStemmer
    from nltk.stem.snowball import SnowballStemmer
    from nltk.stem.wordnet import WordNetLemmatizer
    from nltk.util import ngrams
    from nltk import pos_tag
    from nltk import RegexpParser
    from collections import Counter
    from sklearn.feature_extraction.text import CountVectorizer
    from sklearn.feature_extraction.text import TfidfVectorizer
    import re
    from math import log

    email_filepath = 'C:/Users/JHei/Documents/nlp_email_analysis/data/Emails.csv'
    clean_email_filepath = 'C:/Users/JHei/Documents/nlp_email_analysis/data/cleaned_emails.csv'
    df = load_dataset(email_filepath)
    #Declare and update english wordset
    english_word_set = create_english_wordset()
    #Declare stopwords and punctuation
    stopwords_ = set(stopwords.words('english'))
    punctuation_ = set(string.punctuation)
    raw_text_tokens = tokenize_email_raw_text_line(df)
    raw_text_corpus = corpus_creator(raw_text_tokens)
    subjectline_tokens = tokenize_email_subject_line(df)
    cleaned_df = clean_token_dataframe(raw_text_tokens, subjectline_tokens)
    cleaned_df.to_csv(path_or_buf=clean_email_filepath)


