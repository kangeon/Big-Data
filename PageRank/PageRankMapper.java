import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
  private Text keyOutlink = new Text();
  private Text valueInlinkAndRank = new Text();
  private Text keyInlink = new Text();
  private Text valueOutlinks = new Text();
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  String line = value.toString();
	  //referenced code from stackoverflow.com/questions/11607496/trim-a-string-in-java-to-get-first-word
	  //to extract everything up to first space
	  int firstSpaceIndex = line.indexOf(" ");
	  String inlink = line.substring(0,firstSpaceIndex);
	  String rest = line.substring(firstSpaceIndex+1);
	  String[] remainingValues = rest.split(" ");
	  String initialPageRank = remainingValues[remainingValues.length-1];
	  
	  double initialPageRankDouble = Double.parseDouble(initialPageRank);
	  double numberOfOutlinks = (double) remainingValues.length-1;
	  double pageRankOut = initialPageRankDouble / numberOfOutlinks;
	  String pageRankOutString = String.valueOf(pageRankOut);
	  valueInlinkAndRank.set(inlink + "," + pageRankOutString);
	  
	  StringBuilder originalOutlinks = new StringBuilder();
	  for(int i = 0; i < remainingValues.length-1; i++) {
		  keyOutlink.set(remainingValues[i]);
		  context.write(keyOutlink, valueInlinkAndRank);
		  originalOutlinks.append(remainingValues[i] + " ");
	  }
	  
	  keyInlink.set(inlink);
	  valueOutlinks.set(originalOutlinks.toString());
	  context.write(keyInlink, valueOutlinks);
   }
}