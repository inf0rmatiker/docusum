# Document Summarization using TF/IDF Scores
*Credit to Dr. Sangmi Pallickara for providing the requirements of the program. Assignment details can be viewed at:*

[CSU CS435 Webpage](https://www.cs.colostate.edu/~cs435)

## Summary
This program takes uses Hadoop's MapReduce to take a dataset of Wikipedia articles, and for each article, output a summary of the article. The summary contains the top 3 sentences that best represent the corresponding article. The sentences' importances are calculated using TF/IDF (Term Frequency / Inverse Document Frequency) scores.

## Usage
See [Usage](#usage0)

## Background

### Input
A 1+ GB dataset of Wikipedia articles formatted as follows:

```
...

Langinkoski<====>5538356<====>Langinkoski  Langinkoski is a rapid on the Kymi river in Kotka...

The Sinister Urge (film)<====>5538370<====>The Sinister Urge (film)  The Sinister Urge is a 1960 crime drama ...

Robert Wolfall<====>5538374<====>Robert Wolfall Robert Wolfall was an Anglican ....

...
```

As `articleTitle<====>articleID<====>Article text...`, separated by two `\n` characters.

### Term Frequencies (TF)
Given a single article **j**, we can calculate the *term frequency* of a given term **i**, denoted **TF<sub>ij</sub>** by the following formula:

**TF<sub>ij</sub> = 0.5 + 0.5 ( f<sub>ij</sub> / max<sub>k</sub>f<sub>kj</sub>**

...where **max<sub>k</sub>f<sub>kj</sub>** is the raw frequency of the max term **k** in the article **j**.
This formula protects us from developing a bias towards long documents.

### Inverted Document Frequency
The goal of this **IDF** score is to find the number of documents a term **i** appears in within the total **N** documents in the whole corpus.
Thus, we define **IDF<sub>i</sub>** as:

**IDF<sub>i</sub> = log<sub>10</sub>(N/n<sub>i</sub>)**

...where **n<sub>i</sub>** is the number of documents the term appears in.

### TF.IDF score
We will use both the previous **TF** scores and **IDF** scores to calculate **TF<sub>ij</sub> x IDF<sub>i</sub>** for each term.
Note: The higher the **TF.IDF** score is, the more important it is for summarizing a document. This prevents words like "is" from becoming extremely important for a given article, because they appear commonly corpus-wide.

### Sentence Scores (Sentence.TF.IDF)
We will calculate the **Sentence.TF.IDF** score for each sentence **S<sub>k</sub>**, by summing the top N terms in the sentence which have the highest **TF.IDF** score. In this version, we use N = 5, but this can be tweaked as needed. In the edge case where a sentence is *less than* 5, we simply sum the **TF.IDF** values for the terms present. Additionally, we do not consider duplicate terms in the sentence. 

This summation gives us our **Sentence.TF.IDF** score, which we use to rank the sentence's relevancy to the document.

---

## Methodology and Implementation
Here is an explanation of the MapReduce implementation strategy.

### TF Scores
To find the **TF** scores for each term in each article, we use the following MapReduce job:
- **TFMapper Input**: `<LongWritable key, Text value`, where `value` is `articleTitle<====>articleID<====>articleText...`.
- **TFMapper Output**: `IntWritable key, Text value`, where `key` is the `articleID`, and `value` is in the form `,term,TFscore,termFrequency`.
   - Since we know that the map function is called once per article, we can create a `Map<String, Unigram>` which maps individual terms to a `Unigram` object, which contains the raw `frequency` and `term`.
   - For every term in the article, we:
      - Add it to the `Map<String, Unigram>` with a frequency of 1 if it doesn't already exist, or
      - Increment the existing frequency of the term's `Unigram` by 1.
   - Once the article is finished being read into this `Map<String, Unigram>`, we can calculate the **TF** scores for each of the terms and write them to `context`.
   - We also increment a `Counter` variable, `NUMDOCS` by 1 to count the total number of articles the job comes across.

- **TFReducer Input**: `<IntWritable key, Iterable<Text> values>` where `values` is a list of all the **TF** scores output by the mapper corresponding to the `articleID` denoted by `key`.
- **TFReducer Output**: `<IntWritable key, Text value>` for each of the values in the `Iterable`.

*Example output*:
```
...
34230	,internationally,0.538462,1
34230	,received,0.538462,1
34230	,is,0.615385,3
34230	,in,0.615385,3
34230	,commonly,0.538462,1
34230	,still,0.538462,1
34230	,which,0.538462,1
34230	,yt,0.538462,1
34230	,has,0.538462,1
34230	,from,0.538462,1
...
```

### TF.IDF Scores
To find the **TF.IDF** scores, we use the output from the previous MapReduce job as input for this job:
*Note: we also take in the job Counter enum `NUMDOCS` from the previous job.*

- **IDFMapper Input**: `<LongWritable key, Text value>` where `value` comes in the form `articleId ,term,TFscore,rawFrequency`.
   - Example: `34230	,in,0.615385,3`
- **IDFMapper Output**: `<Text term, Text cleanedInput>` where `cleanedInput` is just the original `value` trimmed of whitespace.
- **IDFReducer Input**: `<Text term, Iterable<Text> values>` where `values` is a list of all the `articleId,term,TFscore,rawFrequency`s for the corresponding term.
- **IDFReducer Output**: `<NullWritable, Text value>` where `outValue` is the `,articleID,term,TFScore,rawFrequency,termFreqByIDFScore`

*Example Output:*
```
...
,1085363,zyuranger,0.533333,1,2.690608
,5398388,zyuranger,0.550000,1,2.774691
,5112538,zyuranger,0.571429,1,2.882798
,6120428,zyuranger,0.545455,1,2.751762
,1589980,zyuranger,0.566667,2,2.858775
,2203370,zyuranger,0.611111,2,3.082990
,173488,zyuranger,0.590909,2,2.981073
,749966,zyuskov,0.515625,1,3.002507
,7722544,zyuskov,0.600000,1,3.493827
,5894536,zyuzicosa,0.501538,1,3.071456
,6516037,zywordis,0.600000,1,3.674445
...
```

### Sentence TF.IDF Scores

To find the **Sentence.TF.IDF** scores, we use the output from the previous Mapreduce Job as input for this job:
*Note: This job takes both the original dataset and the output from the previous job as inputs.*

- **SentenceMapper Input**: `<LongWritable key, Text value>` where `value` comes in the form of either:
   - `,articleID,term,TFScore,rawFrequency,termFreqByIDFScore`, or
   - `articleTitle<====>articleID<====>articleText...` (from the original dataset)
- **SentenceMapper Output**: `<IntWritable articleID, Text value>` where value is either:
   - `;;B;;articleText` if the input is from the original dataset, or
   - `;;A;;term;;termTfIdf` if the input was from the previous job.
- **SentenceReducer Input**: `<IntWritable articleID, Iterable<Text> values>` where values come in as either:
   - the character 'A', followed by the term and **TF.IDF** for that term
   - the character 'B', followed by the raw article text
- **SentenceReducer Output**: `<IntWritable articleID, Text sentences>` where `sentences` are the top M sentences to summarize the article, output in their original order.
   
   The reducer iterates once over all the values for a given article, creating an `Article` which maps all its terms to their **TF.IDF** values. Then, for each sentence in the article text, the top N unique **TF.IDF** scores are summed for the sentence, producing a **Sentence.TF.IDF** score, and stored in a `PriorityQueue<Sentence>`, sorted by their scores. Next, the top M sentences are polled from the sorted queue to represent the article. Finally, the top articles are sorted based on their original positions in the article, and output to context.

*Example output:*

```
...
116090	South Paris, Maine  South Paris is a census-designated place (CDP) located within the town of Paris in Oxford County, Maine, in the United States.  While the CDP refers only to the densely settled area in the southern part of the town of Paris, the entire town is located within the South Paris ZIP code, resulting in many residents referring to the entire town as South Paris. HistoryDuring the 19th-century, the Little Androscoggin River provided water power to operate mills in South Paris, and the village grew up around them. 

...
```

<a name="usage0"></a>
## Usage
Here is an outline of how to run and use the program:

