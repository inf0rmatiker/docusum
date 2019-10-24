package sentences;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import utils.Sentence;
import utils.Unigram;

public class SentenceMapper extends Mapper<LongWritable, Text, IntWritable, Text>  {

  private static Map<Integer, Map<String, Unigram>> articles = new HashMap<>();

  // Sorts sentences by increasing indexes in their article
  public static class IndexComparator implements Comparator<Sentence> {

    @Override
    public int compare(Sentence s1, Sentence s2) {
      if (s1.getIndex() < s2.getIndex()) return -1;
      else if (s1.getIndex() > s2.getIndex()) return 1;
      return 0;
    }

  }

  private List<File> getFileList() {
    List<File> fileList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      fileList.add(new File("./part-r-0000" + i));
    }
    return fileList;
  }

  private Map<String, Unigram> getOrCreate(Integer articleID) {
    if (articles.containsKey(articleID)) {
      return articles.get(articleID);
    }
    else {
      Map<String, Unigram> article = new HashMap<String, Unigram>();
      articles.put(articleID, article);
      return article;
    }
  }

  private List<Sentence> getTopNSentences(String[] sentences, Map<String, Unigram> article, Context context) throws IOException, InterruptedException  {
    List<Sentence> topNSentences = new ArrayList<>();
    PriorityQueue<Sentence> sortedSentences = new PriorityQueue<>();

    // Sorts naturally by Sentence.TF.IDF value
    for (int index = 0; index < sentences.length; index++) {
      String sentence = sentences[index];

      String[] sentenceSplit = sentence.split("\\s+");
      Double sentenceSum = topNWordsSum(sentenceSplit, article, context);

      Sentence articleSentence = new Sentence(sentenceSum, sentence, index);
      sortedSentences.add(articleSentence);
    }

    int limit = (sentences.length < 3) ? sentences.length : 3;
    for (int i = 0; i < limit; i++) {
      Sentence top = sortedSentences.poll();
      topNSentences.add(top);
      //context.write(new IntWritable(-3), new Text("" + top.getTopTermSum()));
    }

    Collections.sort(topNSentences, new IndexComparator());
    return topNSentences;
  }

  private Double topNWordsSum(String[] sentenceTerms, Map<String, Unigram> article, Context context) throws IOException, InterruptedException {
    // sort for top 5 words in the sentence
    // min heap
    PriorityQueue<Double> topFiveWords = new PriorityQueue<>(Collections.reverseOrder());
    Set<String> alreadyConsideredWords = new HashSet<>();
    int counter = 0;
    for (String term: sentenceTerms) {
      term = term.replaceAll("[^a-zA-Z\\d]", "").toLowerCase();
      if (!term.isEmpty() && !alreadyConsideredWords.contains(term)) {
        Unigram unigram = article.get(term);
        if (unigram != null) {
          Double termTF_IDF = unigram.getTf_idf();
          topFiveWords.offer(termTF_IDF);
          alreadyConsideredWords.add(term);
        }
      }
    }

    Double topNSum = 0.0;

    int limit = (topFiveWords.size() < 5) ? topFiveWords.size() : 5;
    for (int i = 0; i < limit; i++) {
      Double top = topFiveWords.poll();
      //context.write(new IntWritable(-2), new Text("" + top));
      topNSum += top;
    }
    //context.write(new IntWritable(-1), new Text("" + topNSum));
    return topNSum;
  }
//
//  @Override
//  protected void setup(Context context) throws IOException, InterruptedException {
//    List<File> fileList = getFileList();
//    for (File f: fileList) {
//      Scanner fscan = new Scanner(f);
//      while(fscan.hasNextLine()) {
//        // In the format ",4265185,week,0.550000,1,1.014804"
//        String nextLine = fscan.nextLine();
//        // In the format [ , 4265185, week, 0.550000, 1, 1.014804 ]
//        String[] splitLine = nextLine.split(",");
//
//        Integer articleID = Integer.parseInt(splitLine[1]);
//        String term       = splitLine[2];
//        Double tf_idf     = Double.parseDouble(splitLine[5]);
//
//        Map<String, Unigram> article = getOrCreate(articleID);
//        article.put(term, new Unigram(term, tf_idf));
//      }
//      fscan.close();
//    }
//  }

//  @Override
//  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//    String line         = value.toString();
//    Integer articleID   = 0; // Default doc ID if it's missing
//    String articleTitle = ""; // Default title if it's missing
//
    /*
     * Either the first line or the last seems to be empty,
     * so we end up skipping that line if it does not contain "<====>"
     */
//    if (line.contains("<====>")) {
//      // Handle title and document ID
//      String[] splitString = line.split("<====>");
//      articleTitle         = splitString[0];
//      articleID            = Integer.parseInt(splitString[1]);
//
//      if (splitString.length == 3) {
//        line = splitString[2];
//        Map<String, Unigram> article = articles.get(articleID);
//        // Skip empty articles
//        if (!line.trim().isEmpty()) {
//          // tokenize sentences
//          String[] originalSentences = line.split("\\.\\s");
//          List<Sentence> topSentences = getTopNSentences(originalSentences, article, context);
//
//          for (Sentence s: topSentences) {
//            context.write(new IntWritable(articleID), new Text(s.getSentence() + ". "));
//          }
//        }
//      }
//    }
//  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line         = value.toString();
    Integer articleID   = 0; // Default doc ID if it's missing
    String articleTitle = ""; // Default title if it's missing

    /*
     * Either the first line or the last seems to be empty,
     * so we end up skipping that line if it does not contain "<====>"
     */
    if (line.contains("<====>")) {
      // Handle title and document ID
      String[] splitString = line.split("<====>");
      articleTitle = splitString[0];
      articleID = Integer.parseInt(splitString[1]);

      if (splitString.length == 3) {
        line = splitString[2];
        context.write(new IntWritable(articleID), new Text(";;B;;" + line));
      }
    }
    else if (line.contains(",")) {
      String[] splitLine = line.split(",");
      if (splitLine.length == 6) {
        articleID = Integer.parseInt(splitLine[1]);
        String term = splitLine[2];
        Double termTfIdf = Double.parseDouble(splitLine[5]);

        String valueOut = String.format(";;A;;%s;;%f", term, termTfIdf);
        context.write(new IntWritable(articleID), new Text(valueOut));
      }
    }
  }
}
