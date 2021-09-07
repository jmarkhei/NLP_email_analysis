

tokenizer = RegexTokenizer(inputCol="Text", outputCol="Tokens", pattern="\\W")
tokenizedDF = tokenizer.transform(textDF)



if __name__ == '__main__':

    from pyspark.ml.feature import Tokenizer, RegexTokenizer
    from pyspark.ml.feature import StopWordsRemover
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import HashingTF, IDF, Tokenizer, Normalizer
    from pyspark.sql.types import ArrayType, StringType
    from pyspark.sql.functions import col
    from pyspark.ml.feature import Word2Vec

    file_path = ' '
 
    processedDF = spark.read.parquet(file_path)

    # Use default stopwords and add "br" to stopwords list
    stopWords = StopWordsRemover().getStopWords() + ["br", "href", "www", "http", "com"]
    stopwordsRemover = StopWordsRemover(inputCol="Tokens", outputCol="CleanTokens", stopWords=stopWords)
    processedDF = stopwordsRemover.transform(tokenizedDF)

    ps = PorterStemmer()
    stem_udf = udf(
    lambda tokens: [ps.stem(token) for token in tokens], ArrayType(StringType())
    )

    # add StemTokens column
    stemmedDF = processedDF.withColumn("StemTokens", stem_udf(col("CleanTokens")))


    # Resulting top 25 tokens
    wordDistNew = (
    processedDF.withColumn("indivTokens", explode(col("CleanTokens")))
    .groupBy("indivTokens")
    .count()
    .sort(col("count").desc())
    )


    hashingTF = HashingTF(inputCol="CleanTokens", outputCol="TermFrequencies")  # Or use CountVectorizer
    idf = IDF(inputCol="TermFrequencies", outputCol="TFIDFScore")
    normalizer = Normalizer(inputCol="TFIDFScore", outputCol="TFIDFScoreNorm", p=2)  # Normalize L2

    # Adding functions into a pipeline to streamline calling process
    tfidfPipeline = Pipeline(stages=[hashingTF, idf, normalizer])
    tfidfModel = tfidfPipeline.fit(processedDF)
    tfidfDF = tfidfModel.transform(processedDF).drop("TFIDFScore")

    # Learn a vector representation for each row of text
    word2Vec = Word2Vec(
    vectorSize=20, minCount=2, inputCol="CleanTokens", outputCol="word2vecEmbedding"
        )
    model = word2Vec.fit(processedDF.limit(10000))

    wvDF = model.transform(processedDF.limit(10000))
    display(wvDF.select("Text", "word2vecEmbedding"))