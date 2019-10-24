package utils;

import java.util.Comparator;

public class Sentence implements Comparable<Sentence> {

  private Double topTermSum;
  private String sentence;
  private Integer index;



  public Sentence(Double topTermSum, String sentence, int originalIndex) {
    this.topTermSum = topTermSum;
    this.sentence = sentence;
    this.index = originalIndex;
  }

  public Double getTopTermSum() {
    return topTermSum;
  }

  public String getSentence() {
    return sentence;
  }

  public Integer getIndex() {
    return index;
  }

  @Override
  public int compareTo(Sentence other) {
    if (topTermSum > other.getTopTermSum()) return -1;
    else if (topTermSum < other.getTopTermSum()) return 1;
    return 0;
  }

  @Override
  public String toString() {
    return "Sentence{" +
        "topTermSum=" + topTermSum +
        ", sentence='" + sentence + '\'' +
        ", index=" + index +
        '}';
  }
}
