package sentences;

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

  // Sorts sentences by increasing indexes in their article
  public static class IndexComparator implements Comparator<Sentence> {

    @Override
    public int compare(Sentence s1, Sentence s2) {
      if (s1.getIndex() < s2.getIndex()) return -1;
      else if (s1.getIndex() > s2.getIndex()) return 1;
      return 0;
    }

  }

  private Double getSentenceTfIdf(String sentence, Article article, Context context) throws IOException, InterruptedException {
    Double sentenceTfIdf = 0.0;
    PriorityQueue<Double> topFiveWords = new PriorityQueue<>(Collections.reverseOrder());
    Set<String> alreadyConsideredWords = new HashSet<>();
    String[] sentenceTerms = sentence.split("\\s+"); // Split the words in a sentence

    for (String term: sentenceTerms) {
      // format the raw term
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

  private void processArticle(String rawArticleText, Article article, Context context) throws IOException, InterruptedException {
    if (!rawArticleText.trim().isEmpty()) {
      String[] splitSentences = rawArticleText.split("\\.\\s");
      PriorityQueue<Sentence> sortedSentences = new PriorityQueue<>();

      for (int sentenceIndex = 0; sentenceIndex < splitSentences.length; sentenceIndex++) {
        String rawSentence   = splitSentences[sentenceIndex];
        Double sentenceTfIdf = getSentenceTfIdf(rawSentence, article, context);
        sortedSentences.add(new Sentence(sentenceTfIdf, rawSentence, sentenceIndex));
      }

      List<Sentence> orderedSentences = new ArrayList<>();
      int limit = Math.min(3, splitSentences.length);
      for (int i = 0; i < limit; i++) {
        Sentence topNthSentence = sortedSentences.poll();
        orderedSentences.add(topNthSentence);
      }

      Collections.sort(orderedSentences, new IndexComparator());
      for (Sentence s: orderedSentences) {
        context.write(new IntWritable(article.id), new Text(s.getSentence()));
      }
    }

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
        // We are working with the article text
        rawArticleText = valueSplit[2];
      }
      else if (valueStr.contains(";;A;;") && valueSplit.length == 4) {
        // We are working with a unigram
        String term = valueSplit[2];
        Double termTfIdf = Double.parseDouble(valueSplit[3]);

        Unigram termUnigram = new Unigram(term, termTfIdf);
        article.addUnigram(term, termUnigram);
        //context.write(new IntWritable(-1), new Text(term));
      }
    }

    if (rawArticleText != null) {
      processArticle(rawArticleText, article, context);
    }

  }




}
