def load_dataset(filepath):
    '''Pulls data in from given path'''
    import pandas as pd 
    df = pd.read_csv(filepath)
    return df

def filter_tokens(sentence):
    '''returns tokens absent stopwords and punctuation'''
    return([w for w in sentence if not w in stopwords_ and not w in punctuation_])

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

def corpus_creator(token_arr):
    '''explodes nested array into word corpus'''
    corpus = []
    for arr in token_arr:
        for word in arr:
            corpus.append(word)
    return corpus



    

if __name__ == '__main__':
    print('hello')