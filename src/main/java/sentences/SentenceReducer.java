package sentences;

import com.google.inject.internal.cglib.core.$LocalVariablesSorter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.Article;
import utils.Sentence;
import utils.Unigram;

public class SentenceReducer extends Reducer<IntWritable, Text, IntWritable, Text> {


  public static class IndexComparator implements Comparator<Sentence> {

    /**
     * Sorts sentences by their original position (index) in the article.
     */
    @Override
    public int compare(Sentence s1, Sentence s2) {
      if (s1.getIndex() < s2.getIndex()) return -1;
      else if (s1.getIndex() > s2.getIndex()) return 1;
      return 0;
    }

  }

  /**
   * Calculates the Sentence.TF.IDF score by summing up the top 5 TF.IDF scores of the words within
   * the sentence. Duplicate words are ignored. If the sentence is less than 5 words, only the
   * unique words in the sentence are summed.
   * @param sentence The Sentence object being considered
   * @param article The Article which contains the sentence. Maintains a lookup table for TF.IDF values
   *                for each term.
   * @return A Sentence.TF.IDF Double representing the relevance score for the sentence in question.
   */
  private Double getSentenceTfIdf(String sentence, Article article) {
    Double sentenceTfIdf = 0.0;
    PriorityQueue<Double> topFiveWords = new PriorityQueue<>(Collections.reverseOrder());
    Set<String> alreadyConsideredWords = new HashSet<>();

    // Split the words in a sentence
    String[] sentenceTerms = sentence.split("\\s+");

    for (String term: sentenceTerms) {
      // format the raw term to remove non-alphanumeric characters
      term = term.replaceAll("[^a-zA-Z\\d]", "").toLowerCase().trim();

      if (!term.isEmpty()) {
        // lookup term in unigrams map to get its TF.IDF score
        Unigram correspondingUnigram = article.unigrams.get(term);

        if (correspondingUnigram != null && !alreadyConsideredWords.contains(term)) {
          topFiveWords.add(correspondingUnigram.getTf_idf());
          alreadyConsideredWords.add(term);
        }
      }
    }

    int limit = Math.min(5, topFiveWords.size());
    for (int i = 0; i < limit; i++) {
      sentenceTfIdf += topFiveWords.poll();
    }

    return sentenceTfIdf;
  }

  /**
   * Processes an article by finding the top 3 sentences which represent the article.
   * @param rawArticleText The article text associated with the article
   * @param article The Article object containing a lookup table for terms and their TF.IDF values
   * @param context The context we are writing the final output to
   * @throws IOException
   * @throws InterruptedException
   */
  private void processArticle(String rawArticleText, Article article, Context context) throws IOException, InterruptedException {
    if (!rawArticleText.trim().isEmpty()) {
      String[] splitSentences = rawArticleText.split("\\.\\s");
      PriorityQueue<Sentence> sortedSentences = new PriorityQueue<>();

      for (int sentenceIndex = 0; sentenceIndex < splitSentences.length; sentenceIndex++) {
        String rawSentence   = splitSentences[sentenceIndex];
        Double sentenceTfIdf = getSentenceTfIdf(rawSentence, article);
        sortedSentences.add(new Sentence(sentenceTfIdf, rawSentence, sentenceIndex));
      }

      String summary = topOrderedSentences(sortedSentences);
      context.write(new IntWritable(article.id), new Text(summary));
    }
  }

  /**
   * Creates a summary of the article by concatenating the top 3 sentences representing that article,
   * while preserving the original ordering.
   * @param sortedSentences A PriorityQueue<Sentence> (Max Heap) of Sentence objects, along with their
   *                        original indexes into the article
   * @return A single String summary of the article
   */
  private String topOrderedSentences(PriorityQueue<Sentence> sortedSentences) {
    StringBuilder summarySentences = new StringBuilder();

    List<Sentence> orderedSentences = new ArrayList<>();
    int limit = Math.min(3, sortedSentences.size());
    for (int i = 0; i < limit; i++) {
      Sentence topNthSentence = sortedSentences.poll();
      orderedSentences.add(topNthSentence);
    }

    // Sort sentences by order they appeared in the article, and restore punctuation
    orderedSentences.sort(new IndexComparator());
    for (Sentence s: orderedSentences) {
      summarySentences.append(s.getSentence());
      summarySentences.append(". ");
    }

    return summarySentences.toString();
  }

  @Override
  protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    Integer articleID = key.get();
    Article article = new Article(articleID);
    String rawArticleText = null;

    for (Text value: values) {
      String valueStr = value.toString();
      String[] valueSplit = valueStr.split(";;");

      if (valueStr.contains(";;B;;") && valueSplit.length == 3) {
        // Process the article text
        rawArticleText = valueSplit[2];
      }
      else if (valueStr.contains(";;A;;") && valueSplit.length == 4) {
        // Process the unigram, adding it to article's HashMap
        String term = valueSplit[2];
        Double termTfIdf = Double.parseDouble(valueSplit[3]);
        article.addUnigram(term, new Unigram(term, termTfIdf));
      }
    }

    if (rawArticleText != null) {
      processArticle(rawArticleText, article, context);
    }

  }




}
