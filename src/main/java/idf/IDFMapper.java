package idf;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IDFMapper extends Mapper<LongWritable, Text, Text, Text> {
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    // Line comes in as e.g.: "4265440	,integral,0.550000,1"
    // Remove all whitespace, split into [ articleID, term, TF_ij, raw_frequency ]
    String cleanInput = value.toString().replaceAll("\\s+", "");
    String[] inputSplit = cleanInput.split(",");
    String term = inputSplit[1];
    context.write(new Text(term), new Text(cleanInput));
  }
}
