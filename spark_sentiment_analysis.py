
from pyspark.sql.types import *
from pyspark.sql.functions import col, udf
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob

analyzer = SentimentIntensityAnalyzer()


def get_nltk_sentiment_score(text):
    return analyzer.polarity_scores(text)["compound"]


# Example usage
get_nltk_sentiment_score("The function will return the sentiment of this sentence!")

@udf("double")
def sentiment_nltk_udf(text):
    return get_nltk_sentiment_score(text)


sentimentDF = processedDF.withColumn("sentimentNLTKScore", sentiment_nltk_udf(col("Text")))

display(sentimentDF.select("Text", "Score", "sentimentNLTKScore"))



def get_textblob_sentiment_score(text):
    sentiment = TextBlob(text).sentiment
    return (sentiment.polarity, sentiment.subjectivity)


# Example usage
get_textblob_sentiment_score("The function will return the sentiment of this sentence!")