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
