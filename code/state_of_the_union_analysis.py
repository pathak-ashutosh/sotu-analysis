from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, count, avg, stddev, col, split, array_sort, concat_ws
from pyspark.sql.types import StringType, ArrayType, IntegerType
from pyspark.sql.window import Window
import re

# Initializing Spark session
spark = SparkSession.builder.appName("StateOfTheUnionAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read the text file from HDFS
speeches_df = spark.read.text("hdfs:///user/apathak2/input/assignment-1/stateoftheunion1790-2021.txt")

# Convert the DataFrame to RDD and then to a list for further processing
speeches_list = speeches_df.rdd.map(lambda row: row[0]).collect()

# Combine the list into a single string
text = "\n".join(speeches_list)

# Split the text into speeches using "***" as the separator
speeches = re.split(r'\*\*\*', text)[1:]

# Create a schema for the DataFrame
schema = StringType()

# Create a DataFrame for the speeches
speeches_df = spark.createDataFrame(speeches, schema=schema)
speeches_df.show()

# Define stopwords
stopwords = set([
        "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves",
        "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their",
        "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was",
        "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and",
        "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between",
        "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off",
        "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any",
        "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than",
        "too", "very", "s", "t", "can", "will", "just", "don", "should", "now", "d", "ll", "m", "o", "re", "ve", "y", "ain", "aren",
        "couldn", "didn", "doesn", "hadn", "hasn", "haven", "isn", "ma", "mightn", "mustn", "needn", "shan", "shouldn", "wasn", "weren", "won", "wouldn",
        "aint", "arent", "couldnt", "didnt", "doesnt", "hadnt", "hasnt", "havent", "isnt", "ma", "mightnt", "mustnt", "neednt", "shant", "shouldnt", 
        "wasnt", "werent", "wont", "wouldnt", "cant", "dont", "im", "ive", "isnt", "youre", "hes", "shes", "its", "were", "theyre", "ive", "youve", "weve"
    ])

# Define UDF to clean the text
def clean_text(text):
    # Remove HTML tags
    cleaned = re.sub('<.*?>', '', text)
    # Remove URLs
    cleaned = re.sub(r'http\S+', '', cleaned)
    # Remove punctuation
    cleaned = re.sub(r'[^\w\s]', '', cleaned)
    # Convert to lower case and split
    words = cleaned.lower().split()
    # Remove stopwords and years
    cleaned_words = [word for word in words if word not in stopwords and not re.match(r'\b\d{4}\b', word)]
    return cleaned_words

clean_text_udf = udf(clean_text, ArrayType(StringType()))

# Apply the cleaning
cleaned_speeches = speeches_df.withColumn("words", clean_text_udf(speeches_df.value))
print("Cleaned speeches:")
cleaned_speeches.show()

# Extract year from speeches and explode words for counting
date_pattern = r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}, (\d{4})\b"
year_extract = udf(lambda speech: int(re.search(date_pattern, speech).group(1)) if re.search(date_pattern, speech) else None, IntegerType())
speeches_with_year = cleaned_speeches.withColumn("year", year_extract(cleaned_speeches.value))
print("Speeches with year:")
speeches_with_year.show()

exploded_speeches = speeches_with_year.select("year", explode("words").alias("word"))
print("Exploded speeches:")
exploded_speeches.show()

# Filter for speeches from 2009 onwards
filtered_speeches = exploded_speeches.filter(exploded_speeches.year >= 2009)

# Group by 4-year windows and compute the count for each word
grouped_speeches = filtered_speeches.groupBy(((filtered_speeches.year - 2009) / 4).cast("int").alias("window"), "word") \
    .agg(count("word").alias("word_count"))

# Compute average and standard deviation in a subsequent transformation
windowSpec = Window.partitionBy("window")
grouped_speeches = grouped_speeches.withColumn("avg_count", avg("word_count").over(windowSpec)) \
    .withColumn("stddev_count", stddev("word_count").over(windowSpec))

# Determine the year following each window
grouped_speeches = grouped_speeches.withColumn("following_year", (grouped_speeches.window * 4) + 2009 + 4)

# Find words that appear in the year following each window with count > avg + 2*stddev
interesting_words = grouped_speeches.filter((grouped_speeches.word_count > (grouped_speeches.avg_count + 2 * grouped_speeches.stddev_count)) &
                                ((grouped_speeches.following_year - 2009) % 4 == 0))

print("Words that appear in the year following each window with count > avg + 2*stddev:")
interesting_words.show()


# ======================================================================================================================

# Filter for speeches from 2009 onwards
filtered_speeches = speeches_with_year.filter(speeches_with_year.year >= 2009)

# Split each speech into sentences and then into words
sentences = filtered_speeches.withColumn("sentences", split(filtered_speeches.value, r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s'))
sentences = sentences.withColumn("sentence", explode(sentences.sentences))
sentences = sentences.withColumn("words", clean_text_udf(sentences.sentence))

# For each sentence, generate word pairs
def generate_pairs(words):
    pairs = [(words[i], words[j]) for i in range(len(words)) for j in range(i+1, len(words))]
    return pairs

generate_pairs_udf = udf(generate_pairs, ArrayType(ArrayType(StringType())))
sentences = sentences.withColumn("word_pairs", generate_pairs_udf(sentences.words))

# Explode the word pairs and group by them to get the counts
pairs_df = sentences.select(explode(sentences.word_pairs).alias("pair"))
grouped_pairs = pairs_df.groupBy("pair").count().filter("count > 10")

# Sort the word pairs to ensure consistent ordering
sorted_pairs = grouped_pairs.withColumn("sorted_pair", array_sort(grouped_pairs.pair))
sorted_pairs = sorted_pairs.withColumn("pair_str", concat_ws("-", sorted_pairs.sorted_pair[0], sorted_pairs.sorted_pair[1]))

# Show 20 frequent pairs of words
sorted_pairs.select("pair_str", "count").show(20)

# ======================================================================================================================

# Compute the total number of sentences
total_sentences = sentences.count()

# Compute frequency of each word
word_freq = sentences.withColumn("word", explode(sentences.words)).groupBy("word").count().withColumnRenamed("count", "word_count")

# Compute pair frequencies
pair_freq = pairs_df.groupBy("pair").count()

# Join the word frequencies to compute individual word probabilities
pair_freq = pair_freq.join(word_freq.withColumnRenamed("word", "word1"), col("word1") == col("pair")[0]).withColumnRenamed("word_count", "word1_count")
pair_freq = pair_freq.join(word_freq.withColumnRenamed("word", "word2"), col("word2") == col("pair")[1]).withColumnRenamed("word_count", "word2_count")

# Calculate the probabilities
pair_freq = pair_freq.withColumn("P_A", col("word1_count") / total_sentences)
pair_freq = pair_freq.withColumn("P_B", col("word2_count") / total_sentences)
pair_freq = pair_freq.withColumn("P_A_and_B", col("count") / total_sentences)

# Calculate lift
pair_freq = pair_freq.withColumn("lift", col("P_A_and_B") / (col("P_A") * col("P_B")))

# Filter pairs with lift > 3.0
high_lift_pairs = pair_freq.filter(pair_freq.lift > 3.0)

high_lift_pairs.select("pair", "lift").show()

# ======================================================================================================================

# Save 'interesting_words' DataFrame sample to a text file
interesting_words.sample(fraction=0.2).rdd.map(lambda row: ",".join(map(str, row))).coalesce(10).saveAsTextFile("hdfs:///user/apathak2/output/assignment-1/interesting_words_sample.txt")

# Save 'sorted_pairs' DataFrame sample to a text file
sorted_pairs.select("pair_str", "count").sample(fraction=0.2).rdd.map(lambda row: ",".join(map(str, row))).coalesce(10).saveAsTextFile("hdfs:///user/apathak2/output/assignment-1/pairs_sample.txt")

# Save 'high_lift_pairs' DataFrame to a text file
high_lift_pairs.select("pair", "lift").rdd.map(lambda row: ",".join(map(str, row))).saveAsTextFile("hdfs:///user/apathak2/output/assignment-1/high_lift_pairs.txt")



spark.stop()
