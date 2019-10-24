package utils;

import org.apache.hadoop.io.Text;

public class Unigram implements Comparable<Unigram> {
  private Integer frequency;
  private String value;
  private Double normalizedFrequency;
  private Double tf_idf;

  public Unigram() {
    this("", 0);
  }

  public Unigram(String value) {
    this(value, 1);
  }

  public Unigram(Text value) {
    String[] splitValue = value.toString().split("\\s");
    this.value = splitValue[0];
    this.frequency = Integer.parseInt(splitValue[1]);
  }

  public Unigram(String value, Integer frequency) {
    this.value = value;
    this.frequency = frequency;
  }

  public Unigram(String value, Double tf_idf) {
    this.tf_idf = tf_idf;
    this.value = value;
  }

  public Integer getFrequency() {
    return frequency;
  }

  public String getValue() {
    return value;
  }

  // Returns the new frequency of the unigram
  public Integer setFrequency(Integer frequency) {
    this.frequency = frequency;
    return frequency;
  }

  // Returns the new incremented value of the unigram
  public Integer incrementFrequency() {
    return ++this.frequency;
  }

  public Text getText() throws NullPointerException {
    return new Text(this.toString());
  }

  public Double getNormalizedFrequency() {
    return normalizedFrequency;
  }

  public Double getTf_idf() {
    return tf_idf;
  }

  // Sort by frequency first, then alphabetic value
  @Override
  public int compareTo(Unigram other) {
    if (this.frequency > other.frequency) return -1;
    if (this.frequency < other.frequency) return 1;
    else {
      return this.value.compareTo(other.value);
    }
  }

  @Override
  public String toString() {
    return String.format("%s %d", value, frequency);
  }
}
