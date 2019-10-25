# Document Summarization using TF/IDF Scores
*Credit to Dr. Sangmi Pallickara for providing the requirements of the program. Assignment details can be viewed at:*

[CSU CS435 Webpage](https://www.cs.colostate.edu/~cs435)

## Summary
This program takes uses Hadoop's MapReduce to take a dataset of Wikipedia articles, and for each article, output a summary of the article. The summary contains the top 3 sentences that best represent the corresponding article. The sentences' importances are calculated using TF/IDF (Term Frequency / Inverse Document Frequency) scores.

## Usage
See [Usage](#usage0)

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

### Sentence Scores (Sentence.TF.IDF)
We will calculate the **Sentence.TF.IDF** score for each sentence **S<sub>k</sub>**, by summing the top N terms in the sentence which have the highest **TF.IDF** score. In this version, we use N = 5, but this can be tweaked as needed. In the edge case where a sentence is *less than* 5, we simply sum the **TF.IDF** values for the terms present. Additionally, we do not consider duplicate terms in the sentence. 

This summation gives us our **Sentence.TF.IDF** score, which we use to rank the sentence's relevancy to the document.

---

## Methodology and Implementation
Here is an explanation of the MapReduce strategy.

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

<a name="usage0"></a>
## Usage
Here is an outline of how to run and use the program:

