package terms;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import utils.Sentence;
import utils.Unigram;

public class TFReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

  @Override
  protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    for (Text value: values) {
      context.write(key, value);
    }
  }

}
