package sentences;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ArticlePartitioner extends Partitioner<IntWritable, Text> {

  @Override
  public int getPartition(IntWritable articleID, Text value, int numReduceTasks) {
    return articleID.get() % 10;
  }
}
