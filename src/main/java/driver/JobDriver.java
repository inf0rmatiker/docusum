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

public class JobDriver {

  // Global article counter
  public static enum DocumentsCount {
    NUMDOCS
  };


  public static void main(String[] args) {
    try {
      /* ========== BEGIN JOB 1 ==========*/
      /* Collects the TF_ij for all the terms and the total number of articles, N */

      Configuration conf = new Configuration();

      // Give the MapRed job a name. You'll see this name in the Yarn webapp.
      Job firstJob = Job.getInstance(conf, "Term Frequencies (Job 1/2)");
      // Current class
      firstJob.setJarByClass(JobDriver.class);
      // Mapper
      firstJob.setMapperClass(TFMapper.class);
      // Reducer
      firstJob.setReducerClass(TFReducer.class);
      // --- Use 5 Reducers ---
      firstJob.setNumReduceTasks(5);
      // Outputs from the Mapper.
      firstJob.setMapOutputKeyClass(IntWritable.class);
      firstJob.setMapOutputValueClass(Text.class);
      // Outputs from Reducer. It is sufficient to set only the following two properties
      // if the Mapper and Reducer has same key and value types. It is set separately for
      // elaboration.
      firstJob.setOutputKeyClass(IntWritable.class);
      firstJob.setOutputValueClass(Text.class);
      // path to input in HDFS
      FileInputFormat.addInputPath(firstJob, new Path(args[0]));
      // path to output in HDFS
      FileOutputFormat.setOutputPath(firstJob, new Path("/cs435/tmp/")); // Output results to intermediate folder
      // Block until the job is completed.
      firstJob.waitForCompletion(true);

      // Comment out to move on to second job
      //System.exit(firstJob.waitForCompletion(true) ? 0 : 1);

      /* ========= END JOB 2 ============ */


      /* ========== BEGIN JOB 2 ========== */
      /* Collects the TF_ij X IDF_i for all the terms */

      // Give the MapRed job a name. You'll see this name in the Yarn webapp.
      Job secondJob = Job.getInstance(conf, "Inverted Document Frequencies (Job 2/2)");
      Counter documentCount = firstJob.getCounters().findCounter(DocumentsCount.NUMDOCS);
      secondJob.getConfiguration().setLong(documentCount.getDisplayName(), documentCount.getValue());
      // Current class.
      secondJob.setJarByClass(JobDriver.class);
      // Mapper
      secondJob.setMapperClass(IDFMapper.class);
      // Reducer
      secondJob.setReducerClass(IDFReducer.class);
      // --- Use 1 Reducer ---
      secondJob.setNumReduceTasks(1);
      // Outputs from the Mapper.
      secondJob.setMapOutputKeyClass(Text.class);
      secondJob.setMapOutputValueClass(Text.class);
      // Outputs from Reducer. It is sufficient to set only the following two properties
      // if the Mapper and Reducer has same key and value types. It is set separately for
      // elaboration.
      secondJob.setOutputKeyClass(NullWritable.class);
      secondJob.setOutputValueClass(Text.class);
      // path to input in HDFS
      FileInputFormat.addInputPath(secondJob, new Path("/cs435/tmp/"));
      // path to output in HDFSargs[0]
      FileOutputFormat.setOutputPath(secondJob, new Path(args[1]));
      // Block until the job is completed.
      System.exit(secondJob.waitForCompletion(true) ? 0 : 1);

      /* ========= END JOB 2 ============ */
    } catch (IOException e) {
      System.err.println(e.getMessage());
    } catch (InterruptedException e) {
      System.err.println(e.getMessage());
    } catch (ClassNotFoundException e) {
      System.err.println(e.getMessage());
    }

  }


}
