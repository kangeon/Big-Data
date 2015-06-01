import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//code taken from @ wiki.apache.org/hadoop/WordCount

public class TokenSearchDriver {

  public static void main(String[] args) throws Exception {

	Job job = new Job();
    job.setJarByClass(TokenSearchDriver.class);
    job.setJobName("Token Search");
    
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	
	job.setMapperClass(TokenSearchMapper.class);
	job.setReducerClass(TokenSearchReducer.class);

	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
    
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}

