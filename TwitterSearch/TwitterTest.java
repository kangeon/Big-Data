import twitter4j.*;

import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;

/**
 * Based off code written by Yusuke Yamamoto provided at http://twitter4j.org/en/code-examples.html
 * Link of specific example: https://github.com/yusuke/twitter4j/blob/master/twitter4j-examples/src/main/java/twitter4j/examples/search/SearchTweets.java
 */
public class TwitterTest {
    /**
     * Usage: java -jar TwitterTest.jar [query] [output file path]
     *
     * @param args "search query" "output file path"
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("java -jar TwitterTest.jar [query] [output file PATH]");
            System.exit(-1);
        }
        
        try {
          File file = new File(args[1]);
          
          if(!file.exists()) {
            file.createNewFile();
          }
          
          FileWriter tweetWriter = new FileWriter(file,true);
          BufferedWriter bufferWriter = new BufferedWriter(tweetWriter);          
        
            Twitter twitter = new TwitterFactory().getInstance();
            Query query = new Query(args[0]);
            query.resultType(Query.RECENT);
            query.setLang("en");
            
            QueryResult result;
            do {
                result = twitter.search(query);
                List<Status> tweets = result.getTweets();
                for (Status tweet : tweets) {
                    bufferWriter.write("@" + tweet.getUser().getScreenName() + " - " + tweet.getText() + "\n");
                }
            } while ((query = result.nextQuery()) != null);
            bufferWriter.close();
            System.exit(0);
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to search tweets: " + te.getMessage());
            System.exit(-1);
        } catch (IOException e) {
          e.printStackTrace();
        }
    }
}