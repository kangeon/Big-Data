import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

	Text outlinksAndPageRank = new Text();
    double sum = 0;
    String outlinks = "";
    for (Text val : values) {
    	String valueString = val.toString();
    	if(valueString.contains(",")){
    		int commaIndex = valueString.indexOf(",");
    		String pageRankString = valueString.substring(commaIndex+1);
    		Double pageRankDouble = Double.valueOf(pageRankString);
    	    sum += pageRankDouble;
    	}
    	else {
    		outlinks = valueString;
    	}
    }
    outlinksAndPageRank.set(outlinks + String.valueOf(sum));
    context.write(key, outlinksAndPageRank);
  }
}