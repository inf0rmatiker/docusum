package utils;

import java.util.HashMap;
import java.util.Map;

public class Article {

  public Integer id;
  public Map<String, Unigram> unigrams;

  public Article(Integer id) {
    this.id = id;
    this.unigrams = new HashMap<>();
  }

  public boolean addUnigram(String term, Unigram u) {
    if (unigrams.containsKey(term)) {
      return false;
    }
    else {
      unigrams.put(term, u);
      return true;
    }
  }

  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    s.append("Article ID: " + id + "\nKeys:\n");
    for (String key: unigrams.keySet()) {
      s.append(key + "\n");
    }
    return s.toString();
  }
}
