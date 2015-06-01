import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRankDriver {

  public static void main(String[] args) throws Exception {
	
	int i = 0;  
	while(i < 3) {
	  Job job = new Job();
      job.setJarByClass(PageRankDriver.class);
      job.setJobName("Page Rank");
    
      //referred to stackoverflow.com/questions/11031785/hadoop-key-value-are-tab-separated-in-the-output-file-how-to-do-it-semicol
      //to change the default tab separation between key/value pairs into spaces
	  Configuration conf = job.getConfiguration();
      conf.set("mapred.textoutputformat.separator", " ");
	
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);
	
	  job.setMapperClass(PageRankMapper.class);
	  job.setReducerClass(PageRankReducer.class);

	  job.setInputFormatClass(TextInputFormat.class);
	  job.setOutputFormatClass(TextOutputFormat.class);
      
	  if(i == 0) {
	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1]+i));
	  }
	  else {
		  FileInputFormat.addInputPath(job, new Path(args[1]+(i-1)));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]+i));
	  }
      job.waitForCompletion(true);
      i++;
    }
  }
}

