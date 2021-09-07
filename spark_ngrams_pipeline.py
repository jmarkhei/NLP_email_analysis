# Apply n-grams to processed DataFrame
from pyspark.ml.feature import NGram
from pyspark.sql.functions import concat, col

ngramDF = processedDF.select("Text", "Tokens", "CleanTokens")

# Add bigram and trigram column
ngram2 = NGram(n=2, inputCol="CleanTokens", outputCol="ngrams2")
ngram3 = NGram(n=3, inputCol="CleanTokens", outputCol="ngrams3")

ngramDF = ngram2.transform(ngramDF)
ngramDF = ngram3.transform(ngramDF)

# Combine tokens, bigrams, and trigrams
ngramDF = ngramDF.withColumn("ngrams", concat(col("tokens"), col("ngrams2"), col("ngrams3")))

display(ngramDF.select("ngrams"))


# Resulting top 25 ngrams
from pyspark.sql.functions import size, split, explode

ngramDist = (
    ngramDF.withColumn("indivNGrams", explode(col("ngrams")))
    .filter(size(split("indivNGrams", " ")) > 1)  # only keep ngrams with n>1
    .groupBy("indivNGrams")
    .count()
    .sort(col("count").desc())
)

display(ngramDist)