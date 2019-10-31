package driver;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import sentences.ArticlePartitioner;
import sentences.SentenceMapper;
import sentences.SentenceReducer;
import sentences.TermMapper;

public class ProfileB {

  private static final int NUM_REDUCERS = 10;

  private static void addInputPaths(Job job, String dataPath) {
    MultipleInputs.addInputPath(job, new Path("/tempOut1"), TextInputFormat.class, TermMapper.class);
    MultipleInputs.addInputPath(job, new Path(dataPath), TextInputFormat.class, SentenceMapper.class);
  }

  public static void main(String[] args) {
    try {

      Configuration conf = new Configuration();

      /* ========== BEGIN JOB 3 ========== */

      // MapReduce job's name, shows up in the Yarn webapp
      Job thirdJob = Job.getInstance(conf, "Profile B (Job 3/3)");
      addInputPaths(thirdJob, args[0]);
      // Current class.
      thirdJob.setJarByClass(ProfileA.class);
      // Mapper
      thirdJob.setMapperClass(SentenceMapper.class);
      // Reducer
      thirdJob.setReducerClass(SentenceReducer.class);
      // Partition records by last digit of article ID
      thirdJob.setPartitionerClass(ArticlePartitioner.class);
      thirdJob.setNumReduceTasks(10);
      // Outputs from the Mapper.
      thirdJob.setMapOutputKeyClass(IntWritable.class);
      thirdJob.setMapOutputValueClass(Text.class);
      // Outputs from Reducer. 
      thirdJob.setOutputKeyClass(IntWritable.class);
      thirdJob.setOutputValueClass(Text.class);
      // path to output in HDFS
      FileOutputFormat.setOutputPath(thirdJob, new Path(args[1]));
      // Block until the job is completed, exiting on completion.
      System.exit(thirdJob.waitForCompletion(true) ? 0 : 1);

      /* ========= END JOB 3 ============ */


    } catch (IOException e) {
      System.err.println(e.getMessage());
    } catch (InterruptedException e) {
      System.err.println(e.getMessage());
    } catch (ClassNotFoundException e) {
      System.err.println(e.getMessage());
    }

  }
}
