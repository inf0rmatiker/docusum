# Document Summarization using TF/IDF Scores

## Summary
This program takes uses Hadoop's MapReduce to take a dataset of Wikipedia articles, and for each article, output a summary of the article. The summary contains the top 3 sentences that best represent the corresponding article. The sentences' importances are calculated using TF/IDF (Term Frequency / Inverse Document Frequency) scores.

## Methodology

### Term Frequencies (TF)
Given a single article **j**, we can calculate the *term frequency* of a given term **i**, denoted **TF<sub>ij</sub>** by the following formula:

**TF<sub>ij</sub> = 0.5 + 0.5 ( f<sub>ij</sub> / max<sub>k</sub>f<sub>kj</sub>**

...where **max<sub>k</sub>f<sub>kj</sub>** is the raw frequency of the max term **k** in the article **j**.
