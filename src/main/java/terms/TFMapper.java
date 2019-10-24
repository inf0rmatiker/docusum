package terms;

import driver.ProfileA.DocumentsCount;
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
      if (splitString.length == 3) {
        line = splitString[2];

        // Skip empty articles
        if (!line.trim().isEmpty()) {
          articles.put(documentId, collectTerms(line));
          context.getCounter(DocumentsCount.NUMDOCS).increment(1); // Increment article count
        }
      }
    }
  }

  /**
   * Creates a map of terms to Unigram objects, each containing the frequency of t/s/bach/k/under/cacaleb/CS435/Assignment_2/src/main/java/driverhe term.
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
      return terms;
    }
    return null;
  }


  /**
   * Increments the Unigram frequency count if it is in the map already, otherwise add it and
   * initialize its count to one.
   * @param terms Map of String terms to Unigram objects
   * @param token The current String token being considered
   * @return The Unigram we either added or incremented
   */
  private Unigram incrementOrAddUnigram(Map<String, Unigram> terms, String token) {
    if (!terms.containsKey(token)) {
      Unigram newUnigram = new Unigram(token);
      terms.put(token, new Unigram(token));
      return newUnigram;
    }

    terms.get(token).incrementFrequency();
    return terms.get(token);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    // Iterate over articles
    for (Integer documentId: articles.keySet()) {
      Map<String, Unigram> article = articles.get(documentId);
      if (article != null) {
        double maxTermFreq = (double) article.get("MAX_FREQ_TERM").getFrequency();

        // Iterate over the terms
        for (String term: article.keySet()) {
          // Don't redundantly write the MAX_FREQ_TERM
          if (!term.equals("MAX_FREQ_TERM")) {
            int termFreq = article.get(term).getFrequency();
            double normalizedTF = 0.5 + 0.5 * (termFreq/maxTermFreq);

            String outValue = String.format(",%s,%f,%d", term, normalizedTF, article.get(term).getFrequency());
            context.write(new IntWritable(documentId), new Text(outValue));
          }
        }
      }

    }
  }
}
