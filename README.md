# Document Summarization using TF/IDF Scores
*Reference*

## Summary
This program takes uses Hadoop's MapReduce to take a dataset of Wikipedia articles, and for each article, output a summary of the article. The summary contains the top 3 sentences that best represent the corresponding article. The sentences' importances are calculated using TF/IDF (Term Frequency / Inverse Document Frequency) scores.

## Background

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

### Sentence.TF.IDF

