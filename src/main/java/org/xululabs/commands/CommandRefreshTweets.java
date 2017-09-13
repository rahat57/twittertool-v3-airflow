package org.xululabs.commands;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.FileUtils;
import org.xululabs.datasources.MysqlApi;
import org.xululabs.datasources.Twitter4jApi;
import org.xululabs.datasources.UtilFunctions;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.RateLimitStatus;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.URLEntity;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;

@Parameters(commandNames = "refreshTweets", commandDescription = "dump tweets from twitter")

public class CommandRefreshTweets extends BaseCommand {

	@Parameter(names = "-id", description = "get directory of files", required = true)
	private String inputFilepath;
	@Parameter(names = "-od", description = "get directory of files", required = true)
	private String ouputFilepath;
	public String getFilePath() {
		return inputFilepath;
	}
	
	public void setFilePath(String filepath) {
		this.inputFilepath = filepath;
	}
	
	public String getOuputFilePath() {
		return ouputFilepath;
	}
	
	public void setOuputFilePath(String ouputFilepath) {
		this.ouputFilepath = ouputFilepath;
	}
    private int uid;

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }
    
    private static Logger log = LogManager.getRootLogger();
	MysqlApi mysql = new MysqlApi();
	UtilFunctions UtilFunctions = new UtilFunctions();
	Twitter4jApi twittertApi = new Twitter4jApi();
	private	String dbName = "metalchirper";
    
	@Override
    public TtafResponse execute() throws Exception {
    	
    	Map<String, Object> consumerKey = null;
    	Twitter twitter = null;
    	ArrayList<Map<String, Object>> tweets = new ArrayList<Map<String, Object>>();
    	TtafResponse ttafResponse = null;
    	int deadConditions = 0;
    	System.err.println("cleaned ouput directory before writting "+UtilFunctions.cleanDirectory(this.getOuputFilePath()));

    	int lmt =0;
    	//getting fileNames in a given directory
    	List<String> fileNames = UtilFunctions.getFileNames(this.getFilePath());
    	
    	consumerKey = mysql.getAuthKeys(dbName);
		
    	mysql.updateTimeStamp(dbName, consumerKey.get("consumerKey").toString());
    	
    	twitter = twittertApi.getTwitterInstance(consumerKey.get("consumerKey").toString(), consumerKey.get("consumerSecret").toString(), consumerKey.get("accessToken").toString(), consumerKey.get("accessTokenSecret").toString());	

    	
    	// iterate over screenNames to index each user data
    	for (String fileName  : fileNames) {
    		
    		String fileParts[] = fileName.split("_");
    		int uId = Integer.parseInt(fileParts[0]);
    		this.setUid(uId);
    		
    		// checking if files exist
    		File file1 = new File(this.getFilePath()+"/"+fileName);
    		
			if (!(file1.exists())) {
				System.err.println("skipping "+uId+" either missing data file");
				continue;
			}
    		
			//loading query keywords
    		List<String> listkeywords = UtilFunctions.loadFile(this.getFilePath()+"/"+fileName);
    		
    		if (listkeywords.size() > 26) {
//    			System.err.println("size "+listkeywords.size());
    			listkeywords = UtilFunctions.pickNRandom(listkeywords, 25);
    			System.err.println("size "+listkeywords.size());
			}
    		
    		String keywords = UtilFunctions.listToString(listkeywords);
    		
    		
        	if ( keywords.isEmpty() || keywords==null || keywords.equals("") ) {
        		
        		System.err.println("nothing to query for tweets : user "+uId);
        		log.error("nothing to query for tweets : user "+uId);
				continue;
			}
        	
        	System.err.println("user "+uId+" queryKeywords "+keywords);
    		int totalcalls=0;
    		long lowestTweetId = Long.MAX_VALUE;
    		
    		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd"); 
    		 Date date = new Date();

    		Query query = new Query(keywords);
    		query.setCount(100);
    		query.since(dateFormat.format(date));
    		int remApiLimits = 180;
    		boolean enterDead = false;
    		do {
    			
    			QueryResult queryResult;
    			try {
    				
    				if (enterDead) {
    					
    					consumerKey.clear();
    					consumerKey = mysql.getAuthKeys(dbName);
    					mysql.updateTimeStamp(dbName, consumerKey.get("consumerKey").toString());
    					twitter = twittertApi.getTwitterInstance(consumerKey.get("consumerKey").toString(), consumerKey.get("consumerSecret").toString(), consumerKey.get("accessToken").toString(), consumerKey.get("accessTokenSecret").toString());	
    					Map<String, RateLimitStatus> rateLimitStatusAppi = twitter.getRateLimitStatus("application");
    					RateLimitStatus	AppiRateLimit = rateLimitStatusAppi.get("/application/rate_limit_status");
    					remApiLimits = AppiRateLimit.getRemaining();
    					enterDead = false;
					}

    				if (remApiLimits < 5) {
    					
    					System.err.println("deadlock condition getting new auth keys ");
    					
    					System.err.println("limit "+remApiLimits);
    					
    					if (remApiLimits < 2 ) {
							
							System.err.println("limits exceeding going to sleep ..!");
							Thread.sleep(800000);
							System.err.println("wokeUp from sleep ..!");
							
						}
    					deadConditions = 0;
    					twitter = null;
    					enterDead = true;
    					continue;	
    				}

    				queryResult = twitter.search(query);
    				remApiLimits = queryResult.getRateLimitStatus().getRemaining();
    				lmt = remApiLimits;
    				totalcalls++;
    				for (Status tweet : queryResult.getTweets()) {
    					Map<String, Object> tweetInfo = new HashMap<String, Object>();
    					tweetInfo.put("id", tweet.getId());
    					tweetInfo.put("tweet", tweet.getText());
    					tweetInfo.put("screenName", tweet.getUser().getScreenName());
    					tweetInfo.put("userId", tweet.getUser().getId());
    					tweetInfo.put("name", tweet.getUser().getName());
    					tweetInfo.put("retweetCount", tweet.getRetweetCount());
    					double friendsCount = tweet.getUser().getFriendsCount();
    					double followersCount = tweet.getUser().getFollowersCount();
    					double ratio = 0;
    					if (friendsCount!=0) {
    						 ratio = (followersCount/friendsCount);
    					}
    					tweetInfo.put("ratio", ratio);
    					tweetInfo.put("followersCount", tweet.getUser().getFollowersCount());
    					tweetInfo.put("friendsCount", tweet.getUser().getFriendsCount());
    					tweetInfo.put("user_image", tweet.getUser().getProfileImageURL());
    					tweetInfo.put("description", tweet.getUser().getDescription());
    					tweetInfo.put("user_location", tweet.getUser().getLocation());
    					tweetInfo.put("tweet_location", tweet.getGeoLocation());
    					tweetInfo.put("time",tweet.getCreatedAt().getTime());
    					String expandedUrl = "";
    					List<String> urls = new ArrayList<String>();
    					for (URLEntity urle : tweet.getURLEntities()) {
    						 expandedUrl = urle.getExpandedURL();
    						 urls.add(urle.getExpandedURL());
    						break;
                    	 	} 
    						tweetInfo.put("externalUrl",expandedUrl);
    						tweetInfo.put("url",urls);

    					tweetInfo.put("date",new SimpleDateFormat("yyyy-MM-dd").format(tweet.getCreatedAt()).toString());
    					tweetInfo.put("timeZone", tweet.getUser().getUtcOffset());
    					tweets.add(tweetInfo);

    					if (tweet.getId() < lowestTweetId) {
    						lowestTweetId = tweet.getId();
    						query.setMaxId(lowestTweetId);
    					}
    					
    				}
    				
    				if ( tweets.size() >= 15000 || totalcalls == 156) {
    					break;
    				}
    				
    			} catch (TwitterException e) {
    				
    				if (e.getErrorCode() == 326) {
    					System.err.println(uid+ " user account blocked skipping");
						continue;
					}
    				
    				System.err.println(e.getErrorCode()+" - "+e.getMessage());

    				if ( e.getErrorCode()==195 ) {
    					System.err.println(e.getMessage());
						System.err.println(" havinf issue in query for user "+uId);
						continue;
					}
    				
    				if (e.getErrorCode() ==32) {
						System.err.println("issue in the keys :"+consumerKey);
					}
    				
    				System.err.println(e.getErrorCode()+" - "+e.getMessage());
			
    			}
    			
    		} while (true);
    		 
    		
    	 ttafResponse = new TtafResponse(tweets);
       	 BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(this.getOuputFilePath()+"/"+uId+"_tweets.txt")));
         write(ttafResponse,bufferedWriter);
         bufferedWriter.close();
         System.err.println("remaining limit "+lmt);
         System.err.println("tweets for "+uId+" Done..!"+" tweetsize "+tweets.size());
         tweets.clear();
         ttafResponse = null;
         
    	}
    	
    	System.err.println("tweets for All Done..!");
    	log.info("tweets for All Done..!");
		return ttafResponse;
    }
	
    @SuppressWarnings("unchecked")
    @Override
    public void write(TtafResponse ttafResponse,BufferedWriter writer) throws Exception  {
    	
      ArrayList<Map<String, Object>> tweets = (ArrayList<Map<String, Object>>) ttafResponse.getResponseData();
     
      String jsonSettings = new Gson().toJson(tweets);
      writer.append(jsonSettings);
      writer.newLine();
      
		
	}    
		
}