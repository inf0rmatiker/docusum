package driver;

import idf.IDFMapper;
import idf.IDFReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import terms.TFMapper;
import terms.TFReducer;

public class ProfileA {
  // Change this to optimize number of reducers for your cluster
  private static final int NUM_REDUCERS = 10;

  // Global article counter
  public static enum DocumentsCount {
    NUMDOCS
  };

  public static void main(String[] args) {
    try {
      /* ========== BEGIN JOB 1 ==========*/
      /* Collects the TF_ij for all the terms and the total number of articles, N */

      Configuration conf = new Configuration();

      // MapReduce job's name, shows up in the Yarn webapp
      Job firstJob = Job.getInstance(conf, " Profile A: Term Frequencies (Job 1/3)");
      // Current class
      firstJob.setJarByClass(ProfileA.class);
      // Mapper
      firstJob.setMapperClass(TFMapper.class);
      // Reducer
      firstJob.setReducerClass(TFReducer.class);
      firstJob.setNumReduceTasks(NUM_REDUCERS);
      // Outputs from the Mapper.
      firstJob.setMapOutputKeyClass(IntWritable.class);
      firstJob.setMapOutputValueClass(Text.class);
      // Outputs from Reducer.
      firstJob.setOutputKeyClass(IntWritable.class);
      firstJob.setOutputValueClass(Text.class);
      // path to input in HDFS
      FileInputFormat.addInputPath(firstJob, new Path(args[0]));
      // path to output in HDFS
      FileOutputFormat.setOutputPath(firstJob, new Path("/tempOut1")); // Output results to intermediate folder
      // Block until job is complete.
      firstJob.waitForCompletion(true);

      /* ========= END JOB 2 ============ */


      /* ========== BEGIN JOB 2 ========== */
      /* Collects the TF_ij X IDF_i for all the terms */

      // Give the MapRed job a name. You'll see this name in the Yarn webapp.
      Job secondJob = Job.getInstance(conf, "Profile A: Inverted Document Frequencies (Job 2/3)");
      // Global NUMDOCS counter from first job
      Counter documentCount = firstJob.getCounters().findCounter(DocumentsCount.NUMDOCS);
      secondJob.getConfiguration().setLong(documentCount.getDisplayName(), documentCount.getValue());
      // Current class
      secondJob.setJarByClass(ProfileA.class);
      // Mapper
      secondJob.setMapperClass(IDFMapper.class);
      // Reducer
      secondJob.setReducerClass(IDFReducer.class);
      secondJob.setNumReduceTasks(NUM_REDUCERS);
      // Outputs from the Mapper.
      secondJob.setMapOutputKeyClass(Text.class);
      secondJob.setMapOutputValueClass(Text.class);
      // Outputs from Reducer. 
      secondJob.setOutputKeyClass(NullWritable.class);
      secondJob.setOutputValueClass(Text.class);
      // path to input in HDFS (intermediate output from first job)
      FileInputFormat.addInputPath(secondJob, new Path("/tempOut1"));
      // path to output (intermediate output for this job)
      FileOutputFormat.setOutputPath(secondJob, new Path("/tempOut2"));
      // Block until the job is completed, exiting on completion.
      System.exit(secondJob.waitForCompletion(true) ? 0 : 1);

    } catch (IOException e) {
      System.err.println(e.getMessage());
    } catch (InterruptedException e) {
      System.err.println(e.getMessage());
    } catch (ClassNotFoundException e) {
      System.err.println(e.getMessage());
    }

  }


}
