# State of the Union Analysis with PySpark
Authored by:  
Ashutosh Pathak and Chaithra Bekal

## Overview
This PySpark program analyzes the State of the Union (SOTU) addresses from 2009 to 2021 to extract valuable insights such as word frequency, interesting word pairs, and word lift. It performs text preprocessing, data analysis, and saves the results to text files.

## Dependencies
- PySpark: The program requires Apache Spark with PySpark to process large-scale data efficiently.
- HDFS: Hadoop Distributed File System is used to read the input text file and save the results.

## Usage
1. Ensure you have Apache Spark with PySpark installed and configured properly.

2. Place the SOTU text file (e.g., `stateoftheunion1790-2021.txt`) in your HDFS directory or modify the file path in the code accordingly.

3. Update the stopwords list in the code if needed. The provided list is a basic set of common English stopwords.

4. Run the program using the following command:
   ```bash
   spark-submit state_of_the_union_analysis.py
    ```

### The program will perform the following steps:
1. Read the SOTU text file.
2. Preprocess the text, including cleaning and extracting years.
3. Analyze word frequencies, interesting word pairs, and word lift.
4. Save the results to text files in HDFS.  

After the program completes, you can access the following result files:
- *interesting_words_sample.txt*: Words that appear with high frequency in the years following each window.
- *pairs_sample.txt*: Frequent word pairs in the sentences that appear > 10 times together.
- *high_lift_pairs.txt*: Word pairs with lift values > 3.0.


## Results
The program was tested on the entire SOTU text file, downloaded directly from the website. The results for the first two tasks are available in the `interesting_words_sample.txt` and `pairs_sample.txt` folders, respectively. The results for the last task with the entire text are available in the `high_lift_pairs.txt` folder.
