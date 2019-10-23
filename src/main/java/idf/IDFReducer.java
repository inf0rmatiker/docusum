package idf;

import driver.JobDriver.DocumentsCount;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Reducer;
import utils.Unigram;

public class IDFReducer extends Reducer<Text, Text, NullWritable, Text> {

  private long articleCount;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    articleCount = context.getConfiguration().getLong(driver.JobDriver.DocumentsCount.NUMDOCS.name(), 0 );
  }

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    // Input comes in as <term, [ "articleID,term,TF_ij,raw_frequency", ... ] >
    List<String> valuesList = new LinkedList<>();
    long articleCountForTerm = 0;
    for (Text value: values) {
      articleCountForTerm++;
      valuesList.add(value.toString());
    }
    if (articleCountForTerm > 0) {
      // Get IDF_i
      Double termIDF = Math.log10((double) articleCount / (double) articleCountForTerm);

      // Output TF_ij X IDF_i for every article
      for (String value: valuesList) {
        // Get the value into the form: [ articleID, term, TF_ij, raw_frequency ]
        String[] inputSplit = value.toString().split(",");
        Double termFreq = Double.parseDouble(inputSplit[2]);
        Integer articleID = Integer.parseInt(inputSplit[0]);

        Double termFreqByIDFScore = termFreq * termIDF;
        String outValue = String.format(",%s,%f", value.toString(), termFreqByIDFScore);

        // Output < NULL, ",articleID,term,TF_ij,raw_frequency,TF-IDF">
        // E.g.: ",4265373,zettner,0.521739,1,1.119719"
        context.write(NullWritable.get(), new Text(outValue));
      }
    }
  }

}
