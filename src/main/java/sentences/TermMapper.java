package sentences;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TermMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] splitLine = line.split(",");
    if (splitLine.length == 6) {
      Integer articleID = Integer.parseInt(splitLine[1]);
      String term = splitLine[2];
      Double termTfIdf = Double.parseDouble(splitLine[5]);

      String valueOut = String.format(";;A;;%s;;%f", term, termTfIdf);
      context.write(new IntWritable(articleID), new Text(valueOut));
    }
  }


}
