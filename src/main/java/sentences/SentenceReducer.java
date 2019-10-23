package sentences;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SentenceReducer extends Reducer<Text, Text, NullWritable, Text> {

}
