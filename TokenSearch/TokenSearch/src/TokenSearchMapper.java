import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Referred to code @ wiki.apache.org/hadoop/WordCount

public class TokenSearchMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private final static IntWritable one = new IntWritable(1);
  private final static IntWritable zero = new IntWritable(0);
  private Text word = new Text();
  private Text word2 = new Text();
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

	  //Initialize counters
	  word2.set("Java");
	  context.write(word2, zero);
	  word2.set("Chicago");
	  context.write(word2, zero);
	  word2.set("Dec");
	  context.write(word2, zero);
	  word2.set("hackathon");
	  context.write(word2, zero);
	  
	  String line = value.toString();
	  StringTokenizer tokenizer = new StringTokenizer(line, "\n");
	  while (tokenizer.hasMoreTokens()) {
		  word.set(tokenizer.nextToken());
		  if(word.toString().toLowerCase().contains("hackathon")) {
			  word2.set("hackathon");
			  context.write(word2, one);
		  }
		  if(word.toString().toLowerCase().contains("dec")) {
			  word2.set("Dec");
			  context.write(word2, one);
		  }
		  if(word.toString().toLowerCase().contains("chicago")) {
			  word2.set("Chicago");
			  context.write(word2, one);
		  }
		  if(word.toString().toLowerCase().contains("java")) {
			  word2.set("Java");
			  context.write(word2, one);
		  }
	  }
   }
}
