package driver;

import driver.ProfileA.DocumentsCount;
import idf.IDFMapper;
import idf.IDFReducer;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
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

  private static void addCacheFiles(Job job) {
    for (int i = 0; i < NUM_REDUCERS; i++) {
      job.addCacheFile(new Path("/cs435/tmp2/part-r-0000" + i).toUri());
    }
  }

  private static void addInputPaths(Job job) {
    MultipleInputs.addInputPath(job, new Path("/cs435/tmp2/"), TextInputFormat.class, TermMapper.class);
    MultipleInputs.addInputPath(job, new Path("/cs435/PA2Dataset/"), TextInputFormat.class, SentenceMapper.class);
  }

  public static void main(String[] args) {
    try {

      Configuration conf = new Configuration();

      /* ========== BEGIN JOB 3 ========== */

      // Give the MapRed job a name. You'll see this name in the Yarn webapp.
      Job thirdJob = Job.getInstance(conf, "Profile B (Job 3/3)");
      addInputPaths(thirdJob);
      //addCacheFiles(thirdJob);
      // Current class.
      thirdJob.setJarByClass(ProfileA.class);
      // Mapper
      thirdJob.setMapperClass(SentenceMapper.class);

//      ChainMapper.addMapper(thirdJob, SentenceMapper.class, LongWritable.class, Text.class, IntWritable.class, Text.class, conf);
//      ChainMapper.addMapper(thirdJob, TermMapper.class, LongWritable.class, Text.class, IntWritable.class, Text.class, conf);

      // Reducer
      thirdJob.setReducerClass(SentenceReducer.class);
      thirdJob.setPartitionerClass(ArticlePartitioner.class);
      // --- Use 1 Reducer ---
      thirdJob.setNumReduceTasks(10);
      // Outputs from the Mapper.
      thirdJob.setMapOutputKeyClass(IntWritable.class);
      thirdJob.setMapOutputValueClass(Text.class);
      // Outputs from Reducer. It is sufficient to set only the following two properties
      // if the Mapper and Reducer has same key and value types. It is set separately for
      // elaboration.
      thirdJob.setOutputKeyClass(IntWritable.class);
      thirdJob.setOutputValueClass(Text.class);
      // path to input in HDFS
      FileInputFormat.addInputPath(thirdJob, new Path(args[0]));
      // path to output in HDFSargs[0]
      FileOutputFormat.setOutputPath(thirdJob, new Path(args[1]));
      // Block until the job is completed.
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
