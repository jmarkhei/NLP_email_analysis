# Databricks notebook source
# MAGIC %pip install flashtext==2.7

# COMMAND ----------

# Databricks notebook source
dbutils.widgets.removeAll()

# Declare widgets for storage account
dbutils.widgets.text("AccountName", "") 

# Containers
dbutils.widgets.text("RawContainer", "") # directory to get raw data
dbutils.widgets.text("EnrichphContainer", "") # directory to find enriched ph

# Folders and Guids
dbutils.widgets.text("RawFolder", "") # folder for raw data
dbutils.widgets.text("EnrichphGuid", "") # eqvuialent to OutputGuid above
dbutils.widgets.text("TopicsOutputGuid", "") # where to write results of topic extraction

# Tokens
dbutils.widgets.text("RawToken", "") # token for raw data
dbutils.widgets.text("EnrichphToken", "") # token for wherever 

dbutils.widgets.text("logfolder", "/job_logs/999/")

# COMMAND ----------

### This is version v2 of RIaccounTopics. Most recent as 01/15/2021

# COMMAND ----------

# Get Account Name
accountName = dbutils.widgets.get("AccountName")

# Containers
rawContainer = dbutils.widgets.get("RawContainer")
enrichContainer = dbutils.widgets.get("EnrichphContainer")

# Folders/Guids
rawFolder = dbutils.widgets.get("RawFolder")
enrichGuid = dbutils.widgets.get("EnrichphGuid") 
topicsOutputGuid = dbutils.widgets.get("TopicsOutputGuid")

# Tokens
rawToken = dbutils.widgets.get("RawToken")
enrichToken = dbutils.widgets.get("EnrichphToken") 


### Reading from Blob
# Env variables for reading from RAW
raw_fs_string = "fs.azure.sas.%s.%s.blob.core.windows.net" % (rawContainer, accountName)
raw_token_template = "https://%s.blob.core.windows.net/%s/{0}?%s" % (accountName, rawContainer, rawToken)
raw_template_input = "wasbs://%s@%s.blob.core.windows.net/%s/{0}" % (rawContainer, accountName, rawFolder)
# Env variables for reading from enrichPH
enrich_fs_string = "fs.azure.sas.%s.%s.blob.core.windows.net" % (enrichContainer, accountName)
enrich_token_template = "https://%s.blob.core.windows.net/%s/{0}?%s" % (accountName, enrichContainer, enrichToken)
enrich_template_input = "wasbs://%s@%s.blob.core.windows.net/%s/{0}" % (enrichContainer, accountName, enrichGuid)

# COMMAND ----------

from itertools import chain
import pyspark.sql.functions as F
from pyspark.sql.types import *
import string
import pandas as pd
import os
import sys
import json
import io
from datetime import datetime, timedelta
import re
import time

beginning_time = time.time()

# Get passed in log folder
log_folder = dbutils.widgets.get("logfolder")
# If it was empty, replace with run_id or 999
if log_folder == "/job_logs/999/" or log_folder == "":
  try:
    run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().get()
    run_id = str(run_id).replace("RunId","").replace("(","").replace(")","")
    log_folder = "/job_logs/" + str(run_id) + "/"
  except:
    log_folder = "/job_logs/" + "999" + "/"
  # create pandas dataframe for logging and create the folder for logging
  logging_output = pd.DataFrame(columns=["time", "type", "text"])
  dbutils.fs.mkdirs(log_folder)
# Otherwise - load parent log folder
else:
  logging_output = sqlContext.read.json(log_folder + "full_log.json").toPandas()
print(log_folder)
 
 
def output_log(log, log_type, text):
  df = pd.DataFrame({"time":str(datetime.now()), "type":log_type, "text": "RIAccountTopics - " + text}, index=[0])
  df = df[list(logging_output.columns)]
  return(log[list(logging_output.columns)].append(df, ignore_index=True))

# COMMAND ----------

csvMeetings_schema = StructType([StructField("MeetingId",StringType(),True),
                                 StructField("ICalUid",StringType(),True),
                                 StructField("Subject",StringType(),True),
                                 StructField("IsRecurring",BooleanType(),True),
                                 StructField("IsCancelled",BooleanType(),True),
                                 StructField("StartTime",TimestampType(),True),
                                 StructField("DurationHours",DoubleType(),True),
                                 StructField("DurationMinutes",DoubleType(),True),
                                 StructField("TotalAccept",IntegerType(),True),
                                 StructField("TotalDecline",IntegerType(),True),
                                 StructField("TotalNoResponse",IntegerType(),True),
                                 StructField("TotalTentativelyAccepted",IntegerType(),True),
                                 StructField("TotalAttendees",IntegerType(),True),
                                 StructField("TotalDoubleBooked",IntegerType(),True),
                                 StructField("TotalNumberOfEmailsDuringMeeting",IntegerType(),True),
                                 StructField("TotalEmailsDuringMeeting",IntegerType(),True)])
 
csvMeetingparticipants_schema = StructType([StructField("MeetingId",StringType(),True),
                                            StructField("PersonHistoricalId",StringType(),True),
                                            StructField("LocalStartTime",TimestampType(),True),
                                            StructField("IsOrganizer",BooleanType(),True),
                                            StructField("IsDoubleBooked",BooleanType(),True),
                                            StructField("Response",StringType(),True),
                                            StructField("DurationMinutesAdjusted",DoubleType(),True),
                                            StructField("NumberOfEmailsDuringMeeting",IntegerType(),True)])
 
csvMails_schema = StructType([StructField("MailId",StringType(),True),
                              StructField("ConversationId",StringType(),True),
                              StructField("Subject",StringType(),True),
                              StructField("SentTime",TimestampType(),True),
                              StructField("SenderTimeSpentInMinutes",DoubleType(),True),
                              StructField("NumberOfRecipients",IntegerType(),True)])
 
csvMailparticipants_schema = StructType([StructField("MailId",StringType(),True),
                                         StructField("PersonHistoricalId",StringType(),True),
                                         StructField("IsSender",BooleanType(),True),
                                         StructField("LocalSentTime",TimestampType(),True),
                                         StructField("PersonTimeSpentInHours",DoubleType(),True),
                                         StructField("PersonTimeSpentInMinutes",DoubleType(),True)])

def check_schema(df, schem, schema_name):
  schema_dict = {i["name"]:i["type"] for i in schem.jsonValue()["fields"]}
  drop_cols = []
  for i in df.columns:
    if(i in schema_dict):
      df = df.withColumn(i, F.col(i).cast(schema_dict[i]))
    else:
      drop_cols.append(i)
  df = df.drop(*drop_cols)
  if(len(drop_cols)>0):
    print("Dropping Columns For " + schema_name + ": ", drop_cols)
  return df

# COMMAND ----------

logging_output = output_log(logging_output, "StdOut", "Reading enriched PersonHistorical data")
logging_output.to_json("/dbfs/" + log_folder + "full_log.json", orient='records')

try:
  ### read enriched person historical
  spark.conf.set(enrich_fs_string, enrich_token_template.format(".csv"))
  person_historical = sqlContext.read.csv(enrich_template_input.format("STG_enrichedPH_FINAL_unfiltered.csv"), header=True, inferSchema=True, escape="\"")
except Exception as e:
  logging_output = output_log(logging_output, "StdErr", str(e))
  logging_output.to_json("/dbfs/" + log_folder + "full_log.json", orient='records')
  raise ValueError(e)

# COMMAND ----------

logging_output = output_log(logging_output, "StdOut", "Reading meeting and mails data. filtering on filters.")
logging_output.to_json("/dbfs/" + log_folder + "full_log.json", orient='records')

try:

  f_meeting = raw_template_input.format("Meetings.csv")
  f_mail = raw_template_input.format("Mails.csv")
  spark.conf.set(raw_fs_string, raw_token_template.format(".csv")) 
  
  ### read meetings
  meetings_data = check_schema(sqlContext.read.csv(f_meeting, header=True,  escape='\"'), csvMeetings_schema, "csvMeetings").filter(~((F.col("TotalAttendees")<=1) 
                                                                                  |(F.col("TotalAttendees")>=250)
                                                                                  |(F.col("DurationMinutes")>=8*60)
                                                                                  |(F.col("IsCancelled") == True)))
    
  if 'Subject' not in meetings_data.columns:
    print('Adding subject column.')
    meetings_data = meetings_data.withColumn("Subject", F.lit("NA"))
  if("IsReal" in meetings_data.columns):
    meetings_data = meetings_data.withColumn('IsReal',F.col("IsReal").cast("float"))
  else:
    meetings_data = meetings_data.withColumn("IsReal", F.lit(1.0).cast("float"))

  ### reading mails
  mails_data = check_schema(sqlContext.read.csv(f_mail, header=True,  escape='\"'), csvMails_schema, "csvMails")
  if 'Subject' not in mails_data.columns:
    print('Adding subject column.')
    mails_data = mails_data.withColumn("Subject", F.lit("NA"))

except Exception as e:
  logging_output = output_log(logging_output, "StdErr", str(e))
  logging_output.to_json("/dbfs/" + log_folder + "full_log.json", orient='records')
  raise ValueError(e)

# COMMAND ----------

logging_output = output_log(logging_output, "StdOut", "Reading Participants data. Filtering for IsOrganizer/IsSender. Joining w Enriched PH")
logging_output.to_json("/dbfs/" + log_folder + "full_log.json", orient='records')

try:
  # meeting
  meeting_participants = check_schema(sqlContext.read.csv(raw_template_input.format("MeetingParticipants.csv"), header=True,  escape='\"'), csvMeetingparticipants_schema, "csvMeetingparticipants")
  # mail
  mail_participants = check_schema(sqlContext.read.csv(raw_template_input.format("MailParticipants.csv"), header=True,  escape='\"'), csvMailparticipants_schema, "csvMailparticipants") 

  ### join with personHistorical
  meeting_participants_joined = meeting_participants.join(person_historical, "PersonHistoricalId", "inner").cache()

  mail_participants_joined = mail_participants.join(person_historical, "PersonHistoricalId", "inner").cache()

except Exception as e:
  logging_output = output_log(logging_output, "StdErr", str(e))
  logging_output.to_json("/dbfs/" + log_folder + "full_log.json", orient='records')
  raise ValueError(e)

# COMMAND ----------

logging_output = output_log(logging_output, "StdOut", "Final join between mails/meeting data + participants/enriched PH")
logging_output.to_json("/dbfs/" + log_folder + "full_log.json", orient='records')

try:
  ### Join participants + mails/meeting
  mails_data = mails_data.join(mail_participants_joined, ["MailId"], "inner")

  meetings_data = meetings_data.join(meeting_participants_joined, ["MeetingId"], "inner")

except Exception as e:
  logging_output = output_log(logging_output, "StdErr", str(e))
  logging_output.to_json("/dbfs/" + log_folder + "full_log.json", orient='records')
  raise ValueError(e)

# COMMAND ----------

# internal mails
internal_mail_count = mails_data.filter(F.col("IsInternal") == True).select("Account", "Subject").count()
print(f'Internal mail count: {internal_mail_count}')
# internal meetings
internal_meeting_count = meetings_data.filter(F.col("IsInternal") == True).select("Account", "Subject").count()
print(f'Internal meeting count: {internal_meeting_count}')

print('')

# external mails
external_mail_count = mails_data.filter(F.col("IsInternal") == False).select("Account", "Subject").count()
print(f'External mail count: {external_mail_count}')
# external meetings
external_meeting_count = meetings_data.filter(F.col("IsInternal") == False).select("Account", "Subject").count()
print(f'External meeting count: {external_meeting_count}')

logging_output = output_log(logging_output, "StdOut", f"External mails: {external_mail_count}; External meetings: {external_meeting_count}")
logging_output.to_json("/dbfs/" + log_folder + "full_log.json", orient='records')

logging_output = output_log(logging_output, "StdOut", f"Internal mails: {internal_mail_count}; Internal meetings: {internal_meeting_count}")
logging_output.to_json("/dbfs/" + log_folder + "full_log.json", orient='records')

# COMMAND ----------

# MAGIC %md Clean Data

# COMMAND ----------

from pyspark.sql.functions import *

def clean_df(df):
  """
  removes single characters and empty subjects.
  """
  df_spark = df\
  .withColumn("Scrubbed_Subject", trim(regexp_replace(lower(col("Subject")), '[^\sa-z]', '')))\
  .withColumn("Scrubbed_Subject", regexp_replace(col("Scrubbed_Subject"), '\t|\n|\s\s+', ' '))\
  .withColumn("Scrubbed_Subject", when(col("Scrubbed_Subject") == "Nan", lit("")).otherwise(col("Scrubbed_Subject")))\
  .withColumn("Scrubbed_Subject", when(size(split(col('Scrubbed_Subject'), ' '))>1,col("Scrubbed_Subject"))\
                      .when(length("Scrubbed_Subject")>45,lit(""))\
                      .otherwise(col("Scrubbed_Subject")))\
  .filter("Scrubbed_Subject !=''")
  return(df_spark)

# COMMAND ----------

logging_output = output_log(logging_output, "StdOut", "Cleaning dataframe. Filtering to only columns of interest.")
logging_output.to_json("/dbfs/" + log_folder + "full_log.json", orient='records')

try:
  ### clean df
  mails, meetings = clean_df(mails_data), clean_df(meetings_data)

  ### get only columns we care about
  mails = mails.select("Subject", "Scrubbed_Subject", "SentTime", "PersonHistoricalId", "Account", "ParentAccount")
  meetings = meetings.select("Subject", "Scrubbed_Subject", "StartTime", "PersonHistoricalId", "Account", "ParentAccount")
  
  ### combined
  joined = mails.union(meetings)

except Exception as e:
  logging_output = output_log(logging_output, "StdErr", str(e))
  logging_output.to_json("/dbfs/" + log_folder + "full_log.json", orient='records')
  raise ValueError(e)

# COMMAND ----------

logging_output = output_log(logging_output, "StdOut", "Creating Cleaned Time Column")
logging_output.to_json("/dbfs/" + log_folder + "full_log.json", orient='records')

try:
  joined_time =  joined.withColumn("Time", concat_ws("-", year(col("SentTime")), month(col("SentTime")), lit("01"))\
                                   .cast("Date")).select("Account", "Time", "Scrubbed_Subject")\
                                   .filter(col("Time").isNotNull())

except Exception as e:
  logging_output = output_log(logging_output, "StdErr", str(e))
  logging_output.to_json("/dbfs/" + log_folder + "full_log.json", orient='records')
  raise ValueError(e)

# COMMAND ----------

### Save as partitioned csv then reload
ResultBlobPath = 'wasbs://' + 'ona' + '@' + accountName + '.blob.core.windows.net' + '/' + topicsOutputGuid

joined_time.repartition(1000).write.csv(ResultBlobPath + "/subject_lines", header='true', escape="\"", mode='overwrite')  
subject_df = sqlContext.read.csv(ResultBlobPath + '/subject_lines', escape="\"", header='true').cache()

# get counts
subject_line_count = subject_df.count()
document_count = subject_df.groupBy("Account", "Time").agg(count("Scrubbed_Subject").alias("SubjectCount")).count()

# COMMAND ----------

stop_words = list([t for t in 'accepted external i me my myself we our ours ourselves you your yours yourself yourselves he him his himself she her hers herself it its itself they them their theirs themselves what which who whom this that these those am is are was were be been being have has had having do does did doing a an the and but if or because as until while of at by for with about against between into through during before after above below to from up down in out on off over under again further then once here there when where why how all any both each few more most other some such no nor not only own same so than too very s t can will just don should now re re: accepted: declined: fw: fw - 1:1 review sync [external] meeting weekly + & # | / @ $ ! % ^ * ( ) . > < ? hi hello hey a b c d e f g h i j k l m n o p q r s t u v w x y z'.split(" ")])

stop_words_bc = sc.broadcast(stop_words)

# COMMAND ----------

from pyspark.sql.functions import col, split, explode
from pyspark.ml import Pipeline
from pyspark.ml.feature import NGram, SQLTransformer, StopWordsRemover

# tokenize
subject_df =  subject_df.withColumn('Tokenized', split(col("Scrubbed_Subject"), " ").cast("array<string>")).select(['Account', 'Time', 'Tokenized'])

# create pipeline to remove stop words, get bigrams, combine with unigrams
stopwords = StopWordsRemover(inputCol="Tokenized", outputCol="Cleaned", stopWords=StopWordsRemover.loadDefaultStopWords("english") + list(string.ascii_lowercase) + stop_words)
ngrams2 = NGram(n=2, inputCol="Cleaned", outputCol="NgramTokens")
combined = SQLTransformer(statement="SELECT *, concat(NgramTokens, Cleaned) AS Tokens FROM __THIS__")
pipe = Pipeline(stages=[stopwords, ngrams2, combined])

# fit and transform
model = pipe.fit(subject_df)
subject_df = model.transform(subject_df).select("Account", "Time", "Tokens")

# COMMAND ----------

# turn each token into its own row
tokenized_df = subject_df.withColumn('Token', explode(col('Tokens'))).drop('Tokens').cache()

# remove empty strings and double check we don't have stopwords
tokenized_df = tokenized_df.where(col('Token') != '').where(~col('Token').isin(stop_words))

# COMMAND ----------

### Summary Statistics
total_tokens = tokenized_df.select("Token").distinct().count()

print(f'Total subject lines: {subject_line_count}')
print(f'Total tokens: {total_tokens}')
print(f'Total documents: {document_count}')

# COMMAND ----------

# MAGIC %md TF-IDF Calculations

# COMMAND ----------

# calculate term frequency - how often does the word appear in the document
tf_df = tokenized_df.groupBy(['Account', 'Time', 'Token']).count().withColumnRenamed('count', 'term_frequency').cache()

# calculate document frequency - in how many documents does this word appear
document_frequency_df = tf_df.groupBy(['Token']).agg(count("term_frequency").alias("document_frequency"))

# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import DoubleType

def calcIdf(doc_frequency, doc_count):
  import math
  return math.log((doc_count) / (doc_frequency))

# calculate IDF
idf_udf = udf(calcIdf, DoubleType())
idf_df = document_frequency_df.withColumn('idf', idf_udf(col('document_frequency'), lit(document_count))).cache()

# calculate TF-IDF
tf_idf_df = tf_df.join(idf_df, ['Token'])
tf_idf_df = tf_idf_df.withColumn('tf_idf', col('term_frequency') * col('idf'))

# write out
tf_idf_df.repartition(1000).write.csv(ResultBlobPath + "/tf_idf_all", header='true', escape="\"", mode='overwrite')  
tf_idf_df = sqlContext.read.csv(ResultBlobPath + '/tf_idf_all', escape="\"", header='true').cache()
tf_idf_df.count()

# COMMAND ----------

logging_output = output_log(logging_output, "StdOut", "Looking for custom horizontal list")
logging_output.to_json("/dbfs/" + log_folder + "full_log.json", orient='records')

HorizontalExists = 0

spark.conf.set(raw_fs_string, raw_token_template.format(".csv")) 
raw_path = "wasbs://%s@%s.blob.core.windows.net/%s" % (rawContainer, accountName, rawFolder)
for i in dbutils.fs.ls(raw_path):
  if i.path == (raw_path + "/horizontal_terms.csv"):
    horizontalTerms = spark.read.csv(i.path, header=False, escape="\"")
    print(f'Custom Horizontal Processes List Found: {horizontalTerms.count()}')
    HorizontalExists = 1

# COMMAND ----------

from flashtext import KeywordProcessor
from pyspark.sql.window import *

def build_kw_processor(word_list):
  """
  inputs
  -----
  word_list: list of keywords
  
  returns
  -----
  kwp: KeywordProcessor object with loaded words
  """
  kwp = KeywordProcessor()
  for word in word_list:
    kwp.add_keyword(word)
  return kwp

class PhraseParser(object):
  def __init__(self, kwp):
    self._kwp = kwp
  
  def search_for_keywords(self, string):
    result = kwp.extract_keywords(string)
    if len(result) > 0:
      return True
    else:
      return False

    
if HorizontalExists:
  word_list = horizontalTerms.select("_c0").collect()
  word_list = list(set([x['_c0'].lower() for x in word_list]))
  
  kwp = build_kw_processor(word_list)
  pp = PhraseParser(kwp)
  keyword_searcher = udf(pp.search_for_keywords, BooleanType())

  tf_idf_df = (tf_idf_df
                     .withColumn('Keep', keyword_searcher(tf_idf_df.Token))
                     .filter(F.col("Keep")==True)
                    )
else:
  # get top 15
  w = Window.partitionBy("Account", "Time").orderBy(col("tf_idf").desc())
  tf_idf_df = tf_idf_df.withColumn("rn", row_number().over(w))\
                .filter(col("rn") <= 15).drop("rn")\
                .orderBy("Account", "Time", "tf_idf")
  

final_df = tf_idf_df.select("Account", "Time", "Token", "tf_idf")\
            .withColumnRenamed("Account", "account")\
            .withColumnRenamed("Time", "time")\
            .withColumnRenamed("Token", "word")\
            .withColumnRenamed("tf_idf", "weight")

# COMMAND ----------

enrich_template_output = "wasbs://%s@%s.blob.core.windows.net/%s/{0}" % (enrichContainer, accountName, topicsOutputGuid)

def df_output_blob(df):
    outFolder = df.name
    outPath = enrich_template_output.format(outFolder)
    df.repartition(1).write.csv(outPath, header='true', escape="\"")   
    
    # Copy file from outFolder to central working directory
    try:
      fullLS = dbutils.fs.ls(outPath)
      for i in fullLS:
        partLS = i
        if 'part-00000' in str(partLS[1]):
          outFileName = partLS[1]
          outFileLocation = outPath +'/'+ outFileName
          newFileLocation =  enrich_template_output.format(df.name + '.csv')
          dbutils.fs.mv(outFileLocation, newFileLocation)
          print ('File moved successfully: ', newFileLocation)
    except Exception as e:
      print ("Error moving file. Error: ", e) 
  
    # clean up old files
    try:
      dbutils.fs.rm(outPath , recurse=True)
      print ('Work Folder deleted: ', outPath)
      return newFileLocation
    except Exception as e:
      print ("Error Deleting work File or Folder. Error: ", e)

# COMMAND ----------

# write out results
spark.conf.set(enrich_fs_string, enrich_token_template.format(".csv"))
final_df.name = "account_topics"
df_output_blob(final_df)

# COMMAND ----------

sqlContext.clearCache()
