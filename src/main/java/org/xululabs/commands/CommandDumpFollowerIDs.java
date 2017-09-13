package org.xululabs.commands;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.xululabs.datasources.UtilFunctions;
import org.xululabs.datasources.MysqlApi;
import org.xululabs.datasources.Twitter4jApi;

import twitter4j.IDs;
import twitter4j.Twitter;
import twitter4j.TwitterException;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;
@Parameters(commandNames = "dumpFollowerIDs", commandDescription = "use to Dump Followers IDs")

public class CommandDumpFollowerIDs extends BaseCommand {
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
	
	private static Logger log = LogManager.getRootLogger();
	MysqlApi mysql = new MysqlApi();
	UtilFunctions UtilFunctions = new UtilFunctions();
	Twitter4jApi twittertApi = new Twitter4jApi();
	private String dbName = "metalchirper";
	int	deadCondition = 0;
	
    @Override
    public TtafResponse execute() throws TwitterException, IOException, ClassNotFoundException, SQLException, InterruptedException {
    
    	Map<String, Object> consumerKey = null;
		Twitter twitter = null;
		TtafResponse ttafResponse = null;
		BufferedWriter bufferedWriter = null;
		System.err.println("cleaned ouput directory before writting "+UtilFunctions.cleanDirectory(this.getOuputFilePath()));

		
		// Function to read files in directory getting all files in directory
		List<String> fileNames = UtilFunctions.getFileNames(this.getFilePath());
		Set<String> allScreenNames = new HashSet<String>();
		
		// making all screenNames unique to avoid from duplication
		for (int i = 0; i < fileNames.size(); i++) {
			
			List<String> screenNames = UtilFunctions.loadFile(this.getFilePath() + "/"+ fileNames.get(i));
			for (String screenName : screenNames) {
				allScreenNames.add(screenName);
			}
			
		}
	
		// getting Authentication keys from Database
					consumerKey = mysql.getAuthKeys(dbName);
					
					//updating database  table for timeStamp so it can be used after all others are consumed
					mysql.updateTimeStamp(dbName, consumerKey.get("consumerKey").toString());
					
					twitter = twittertApi.getTwitterInstance(consumerKey.get("consumerKey").toString(),consumerKey.get("consumerSecret").toString(), consumerKey
							.get("accessToken").toString(),consumerKey.get("accessTokenSecret").toString());

		
		//iteration over each screenNames and dumping Follower Ids for each
		for (String screenName : allScreenNames) {
					
			ArrayList<String> followersIds = new ArrayList<String>();
			int remApiLimits = 15;
			long cursor = -1;
			IDs followerIDs = null;
			long[] followerIds = null;
			boolean enteredDead=false;
			do {
			
				try {

					if (remApiLimits < 2) {
						
						System.err.println("changing Auth keys limit "+remApiLimits);
						consumerKey = mysql.getAuthKeys(dbName);
						
						mysql.updateTimeStamp(dbName, consumerKey.get("consumerKey").toString());
						
						twitter = twittertApi.getTwitterInstance(consumerKey.get("consumerKey").toString(),consumerKey.get("consumerSecret").toString(), consumerKey
								.get("accessToken").toString(),consumerKey.get("accessTokenSecret").toString());
						deadCondition = 0;
					}

					followerIDs = twitter.getFollowersIDs(screenName, cursor);
					followerIds  =  followerIDs.getIDs();
					cursor = followerIDs.getNextCursor();
					for (int i = 0; i < followerIds.length; i++) {
						
						followersIds.add(Long.toString(followerIds[i]));
				
					}
					
					cursor = followerIDs.getNextCursor();
					remApiLimits = followerIDs.getRateLimitStatus().getRemaining();

				} catch (TwitterException e) {
					deadCondition++;
					
					if (e.getErrorCode() == 88) {
						
						if (deadCondition == 4) {
							System.err.println("sleeping for 15 minutes !");
							Thread.sleep(800000);
							deadCondition = 0;
						}
						
						System.err.println("deadlock condition getting new auth keys ");
						
//						consumerKey = mysql.getAuthKeys(dbName);
						
//						mysql.updateTimeStamp(dbName, consumerKey.get("consumerKey").toString());
						
//						twitter = twittertApi.getTwitterInstance(consumerKey.get("consumerKey").toString(),consumerKey.get("consumerSecret").toString(), consumerKey
//								.get("accessToken").toString(),consumerKey.get("accessTokenSecret").toString());
						
					}
				}

			} while ((cursor != 0));

			ttafResponse = new TtafResponse(followersIds);
			bufferedWriter = new BufferedWriter(new FileWriter(new File(this.getOuputFilePath() + "/" + screenName+ "=followerIds")));
			write(ttafResponse, bufferedWriter);
			bufferedWriter.close();
			System.err.println("Dumping Follower ids for " + screenName + " Done..! size "+followersIds.size());
		}
		ttafResponse = null;
		System.out.println("dumping Follower ids Done For All  ..!");
		return ttafResponse;
		
    }

    @SuppressWarnings("unchecked")
    @Override
    public void write(TtafResponse ttafResponse, BufferedWriter writer) throws IOException {
        List<String> followersids = (List<String>) ttafResponse.getResponseData();
      
            String jsonSettings = new Gson().toJson(followersids);
            writer.append(jsonSettings);
            writer.newLine();
    }

  
}
