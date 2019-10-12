package terms;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.Unigram;

public class TFMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

  // Maps Document IDs to their unigram collections
  Map<Integer, Map<String, Unigram>> articles = new HashMap<>();

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line         = value.toString();
    Integer documentId  = 0; // Default doc ID if it's missing
    String articleTitle = ""; // Default title if it's missing


    /*
     * Either the first line or the last seems to be empty,
     * so we end up skipping that line if it does not contain "<====>"
     */
    if (line.contains("<====>")) {
      // Handle title and document ID
      String[] splitString = line.split("<====>");
      articleTitle  = splitString[0];
      documentId    = Integer.parseInt(splitString[1]);
      line          = splitString[2];

      articles.put(documentId, collectTerms(line));
    }
  }

  /**
   * Creates a map of terms to Unigram objects, each containing the frequency of the term.
   * Also adds an entry, "MAX_FREQ_TERM", referencing the Unigram of maximum frequency for the given article.
   * @param article The stringified article text
   * @return A map of terms
   */
  private Map<String, Unigram> collectTerms(String article) {
    // Create data structure to store frequencies of all the words in this article
    Map<String, Unigram> terms = new HashMap<>();

    // The term of maximum frequency within the current article
    Unigram maxFrequencyTerm = null;

    // Tokenize into words delimited by whitespace
    StringTokenizer tokenizer = new StringTokenizer(article);

    // Emit <documentID, word> pairs.
    while (tokenizer.hasMoreTokens()) {
      // Grab a token, remove all punctuation, and lower-case it
      String token = tokenizer.nextToken().trim().replaceAll("[^a-zA-Z0-9]", "").toLowerCase();
      if (!token.isEmpty()) {
        // Update the maxFrequencyTerm, if applicable
        Unigram currentTerm = incrementOrAddUnigram(terms, token);
        if (maxFrequencyTerm == null || currentTerm.getFrequency() > maxFrequencyTerm.getFrequency()) {
          maxFrequencyTerm = currentTerm;
        }
      }
    }

    if (maxFrequencyTerm != null) {
      terms.put("MAX_FREQ_TERM", maxFrequencyTerm);
    }
    return terms;
  }


  // Add the token to the frequencies map if absent, otherwise increment the token's count
  // Returns the corresponding Unigram
  private Unigram incrementOrAddUnigram(Map<String, Unigram> terms, String token) {
    if (!terms.containsKey(token)) {
      return terms.put(token, new Unigram(token));
    }

    terms.get(token).incrementFrequency();
    return terms.get(token);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    // Iterate over articles
    for (Integer documentId: articles.keySet()) {
      Map<String, Unigram> article = articles.get(documentId);

      int maxTermFreq = article.get("MAX_FREQ_TERM").getFrequency();

      // Iterate over the terms
      for (String term: article.keySet()) {
        int termFreq = article.get(term).getFrequency();
        double termFrequency = 0.5 + 0.5 * (termFreq/maxTermFreq);
      }

    }
  }
}
