package org.xululabs.javaservices;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.search.sort.SortOrder;
import org.xululabs.datasources.ElasticsearchApi;

public class SearchingServer extends AbstractVerticle {
	private static Logger log = LogManager.getRootLogger();
	HttpServer server;
	Router router;
	String host;
	int port;
	ElasticsearchApi elasticsearch;
	String esHost;
	String esIndex;
	String Index;
	int esPort;
	int documentsSize;
	int bulkSize = 1000;

	/**
	 * constructor use to initialize values
	 */
	 public  SearchingServer()  {
			this.host = "localhost";
			this.port = 8181;
			this.elasticsearch = new ElasticsearchApi();
			this.esHost = "localhost";
			this.esPort = 9300;
			this.esIndex = "twitter";
			this.Index = "user";
			this.documentsSize = 500;	
	}

	/**
	 * Deploying the verical
	 */
	@Override
	public void start() {	

		server = vertx.createHttpServer();
		router = Router.router(vertx);
		// Enable multipart form data parsing
		router.route().handler(BodyHandler.create());
		router.route().handler(
				CorsHandler.create("*").allowedMethod(HttpMethod.GET)
						.allowedMethod(HttpMethod.POST)
						.allowedMethod(HttpMethod.OPTIONS)
						.allowedHeader("Content-Type, Authorization"));
		
		// registering different route handlers
		this.registerHandlers();

		//portConnection.getPort()
		server.requestHandler(router::accept).listen(port);
		
	}

	/**
	 * For Registering different Routes
	 */
	public void registerHandlers() {
		
		router.route(HttpMethod.GET, "/").blockingHandler(this::welcomeRoute);
		router.route(HttpMethod.POST, "/deleteTweets").blockingHandler(this::deleteTweets);
		router.route(HttpMethod.POST, "/deleteIndex").blockingHandler(this::deleteIndex);
		router.route(HttpMethod.POST, "/deleteInfluencer").blockingHandler(this::deleteInfluence);
		router.route(HttpMethod.POST, "/getMappings").blockingHandler(this::getMappings);
		router.route(HttpMethod.POST, "/search").blockingHandler(this::search);
		router.route(HttpMethod.POST, "/generalSearch").blockingHandler(this::generalSearch);
		router.route(HttpMethod.POST, "/homeSearch").blockingHandler(this::homeSearch);
		router.route(HttpMethod.POST, "/unfollowedFollowers").blockingHandler(this::unfollowedFollowers);
		router.route(HttpMethod.POST, "/rangeFilter").blockingHandler(this::rangeFilter);
		router.route(HttpMethod.POST, "/searchUser").blockingHandler(this::searchUser);
		router.route(HttpMethod.POST, "/searchUserRelation").blockingHandler(this::searchUserRelation);
		router.route(HttpMethod.POST, "/searchUserInfluence").blockingHandler(this::searchUserInfluence);
		router.route(HttpMethod.POST, "/mutualRelation").blockingHandler(this::mutualRelation);
		router.route(HttpMethod.POST, "/stickyRelation").blockingHandler(this::stickyRelation);
		
	}

	/**
	 * welcome route
	 * 
	 * @param routingContext
	 */

	public void welcomeRoute(RoutingContext routingContext) {
		routingContext.response().end("<h1>Welcome To Twitter Tool</h1>");
	}
	
	/**
	 * use to delete documents
	 * 
	 * @param routingContext
	 */
	public void deleteTweets(final RoutingContext routingContext) {
		  Thread t=new Thread() {
		   public void run() { 
			  deleteTweetsBlocking(routingContext);
		   }
		  };
		  t.setDaemon(true);
		  t.start();
		 }
	/**
	 * use to delete index
	 * 
	 * @param routingContext
	 */
	public void deleteIndex(final RoutingContext routingContext) {
		  Thread t=new Thread() {
		   public void run() { 
			   deleteIndexBlocking(routingContext);
		   }
		  };
		  t.setDaemon(true);
		  t.start();
		 }
	
	/**
	 * use to delete documents
	 * 
	 * @param routingContext
	 */
	public void deleteInfluence(final RoutingContext routingContext) {
		  Thread t=new Thread() {
		   public void run() { 
			   deleteInfluenceBlocking(routingContext);
		   }
		  };
		  t.setDaemon(true);
		  t.start();
		 }
	
	/**
	 * use to delete documents
	 * 
	 * @param routingContext
	 */
	public void getMappings(final RoutingContext routingContext) {
		  Thread t=new Thread() {
		   public void run() { 
			   getMappingsBlocking(routingContext);
		   }
		  };
		  t.setDaemon(true);
		  t.start();
		 }
	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void search(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				searchBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void generalSearch(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				generalSearchBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void mutualRelation(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				mutualRelationBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void stickyRelation(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				stickyRelationBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void homeSearch(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				homeSearchBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void unfollowedFollowers(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				unfollowedFollowersBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void rangeFilter(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				rangeFilterBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to search userRelation
	 * 
	 * @param routingContext
	 */
	public void searchUserRelation(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				searchUserRelationBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to search User documents
	 * 
	 * @param routingContext
	 */
	public void searchUser(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				searchUserBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to search User documents
	 * 
	 * @param routingContext
	 */
	
	public void searchUserInfluence(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				searchUserInfluenceBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}

	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void generalSearchBlocking(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();
		
		String keywords = (routingContext.request().getParam("keyword") == null) ? "": routingContext.request().getParam("keyword");
		String searchIn = (routingContext.request().getParam("searchIn") == null) ? "screenName": routingContext.request().getParam("searchIn");
		String size = (routingContext.request().getParam("size") == null) ? "500": routingContext.request().getParam("size");
		String page = (routingContext.request().getParam("page") == null) ? "0": routingContext.request().getParam("page");
		String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription");
		String sortOn = (routingContext.request().getParam("sort") == null) ? "time": routingContext.request().getParam("sort");
		String orderIn = (routingContext.request().getParam("order") == null) ? "": routingContext.request().getParam("order");

		SortOrder order = SortOrder.DESC;
		if (!orderIn.isEmpty() && orderIn.equalsIgnoreCase("0")) {
			order = SortOrder.ASC;
		}
		int pageNo = 0;
		if (! size.isEmpty() || ! page.isEmpty()) {
			 pageNo = Integer.parseInt(page);
			this.documentsSize = Integer.parseInt(size);
		}
		
		try {

			String[] searchKeywords = mapper.readValue(keywords, String[].class);
			long start = System.currentTimeMillis();
			ArrayList<Map<String, Object>> documents = this.elasticsearch.searchDocuments(elasticsearch.getESInstance(esHost, esPort),indexEncription,searchIn, searchKeywords,pageNo, documentsSize);
			if (documents.size() >= 1) {
				Map<String, Object> totalCount = documents.get(0);
				documents.remove(0);
				responseMap.put("size", totalCount.get("totalCount"));
			}
			
			responseMap.put("status", "success");
			responseMap.put("documents", documents);
			
			long end = System.currentTimeMillis()-start;
			log.info("time taken for tweets search total "+end);
			response = mapper.writeValueAsString(responseMap);

		} catch (Exception e) {
//			log.error(e);
			response = "{\"status\" : \"error\", \"msg\" :" +e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

	}
	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void searchBlocking(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();
		
		String keyword = (routingContext.request().getParam("keyword") == null) ? "cat": routingContext.request().getParam("keyword");
		String searchIn = (routingContext.request().getParam("searchIn") == null) ? "tweet": routingContext.request().getParam("searchIn");
		String size = (routingContext.request().getParam("size") == null) ? "500": routingContext.request().getParam("size");
		String page = (routingContext.request().getParam("page") == null) ? "0": routingContext.request().getParam("page");
		String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription");
		String sortOn = (routingContext.request().getParam("sort") == null) ? "time": routingContext.request().getParam("sort");
		String orderIn = (routingContext.request().getParam("order") == null) ? "": routingContext.request().getParam("order");

		SortOrder order = SortOrder.DESC;
		if (!orderIn.isEmpty() && orderIn.equalsIgnoreCase("0")) {
			order = SortOrder.ASC;
		}
		int pageNo = 0;
		if (! size.isEmpty() || ! page.isEmpty()) {
			 pageNo = Integer.parseInt(page);
			this.documentsSize = Integer.parseInt(size);
		}
		
		try {
			// generating types which are created in index so we can get require data
			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			Calendar cal1 = Calendar.getInstance();
			Calendar cal2 = Calendar.getInstance();
		    cal1.add(Calendar.DATE, -1);
		    String oneDayPreviousDate=  formatter.format(cal1.getTime()).toString();
		    cal2.add(Calendar.DATE, -2);
		    String secondDayDate =  formatter.format(cal2.getTime()).toString();
			Date date = new Date();
			String types[] ={formatter.format(date),oneDayPreviousDate,secondDayDate};
			String[] fields = mapper.readValue(searchIn, String[].class);
			long start = System.currentTimeMillis();
			ArrayList<Map<String, Object>> documents = this.elasticsearch.searchTweetDocuments(elasticsearch.getESInstance(esHost, esPort), indexEncription,types, fields, keyword,pageNo, documentsSize,sortOn,order);
				if (documents.size() >= 1) {
					Map<String, Object> totalCount = documents.get(0);
					documents.remove(0);
					responseMap.put("size", totalCount.get("totalCount"));
					}
			
			responseMap.put("status", "success");
			responseMap.put("documents", documents);
			
			long end = System.currentTimeMillis()-start;
			log.info("time taken for tweets search total "+end);
			response = mapper.writeValueAsString(responseMap);

		} catch (Exception e) {
//			log.error(e);
			response = "{\"status\" : \"error\", \"msg\" :" +e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

	}
	
	/**
	   * use to delete tweets by Date 
	   * 
	   * @param routingContext
	   */
	
	  public void deleteInfluenceBlocking(RoutingContext routingContext) {
	    Map<String, Object> responseMap = new HashMap<String, Object>();
	    String response;
	    
	    ObjectMapper mapper = new ObjectMapper();
	    String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription");
	    String userScreenName = (routingContext.request().getParam("userScreenName") == null) ? "": routingContext.request().getParam("userScreenName").toLowerCase();
		String influenceScreenName = (routingContext.request().getParam("influenceScreenName") == null) ? "": routingContext.request().getParam("influenceScreenName").toLowerCase();
		String getuserObject[] = {"userScreenName"};
		ArrayList<String> commonRelation = new ArrayList<String>();
		ArrayList<String> nonCommonRelation = new ArrayList<String>();
		try {
			
			
		ArrayList<Map<String, Object>> documents = this.elasticsearch.searchUserRelationDocuments(elasticsearch.getESInstance(esHost, esPort),indexEncription, getuserObject, userScreenName,0, 1);
	    if (documents.size() >= 1 ) {
	    	for (String field : documents.get(0).keySet()) {
				if (field.equalsIgnoreCase(userScreenName+influenceScreenName+"FollowerRelation") || field.equalsIgnoreCase(userScreenName+influenceScreenName+"NonFollowerRelation")) {
					commonRelation = (ArrayList<String>) documents.get(0).get(userScreenName+influenceScreenName+"FollowerRelation");
			    	nonCommonRelation = (ArrayList<String>) documents.get(0).get(userScreenName+influenceScreenName+"NonFollowerRelation");
					
					
						}
					}
	    	
		} 

	    // deleting influencer type data for common and then type
	    if (commonRelation.size() >= 1) {
	    	 deleteFromES(indexEncription, userScreenName+influenceScreenName+"commonfollowers", commonRelation); 
		}
	    
	    boolean type1 = this.elasticsearch.deleteOnlyMaping(this.elasticsearch.getESInstance(this.esHost, this.esPort), indexEncription,userScreenName+influenceScreenName+"commonfollowers");
		
	    
	   // deleting influencer type data for NonCommon and then type
	    if (nonCommonRelation.size() >= 1) {
	    	deleteFromES(indexEncription, userScreenName+influenceScreenName+"noncommonfollowers", nonCommonRelation);

		}
	   	  boolean type2 = this.elasticsearch.deleteOnlyMaping(this.elasticsearch.getESInstance(this.esHost, this.esPort), indexEncription,userScreenName+influenceScreenName+"noncommonfollowers");
	   	  boolean success = false;
	   if (type1 && type2 ) {
			success = true;
		}
	     
	      if (success) {
	    	  responseMap.put("status", "true");
		}
	      else {
	    	  responseMap.put("status", "false");
		}
	      
	      response = mapper.writeValueAsString(responseMap);

	    } catch (Exception e) {
	    	log.error(e.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

	      response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
	          + "}";
	    }

	    routingContext.response().end(response);

	  }
	  /**
		 * use to delete records in ES
		 * 
		 * @param friends followers ids
	 * @throws Exception 
		 */
		public void deleteFromES(String indexName,String type,ArrayList<String> ids) throws Exception {
			TransportClient client = this.elasticsearch.getESInstance(this.esHost, this.esPort);

			BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
			for (String id : ids) {
				bulkRequestBuilder.add(client.prepareDelete(indexName,type,id));
			}
			bulkRequestBuilder.setRefresh(true).execute().actionGet();

			client.close();
		}
	/**
	   * use to delete tweets by Date 
	   * 
	   * @param routingContext
	   */
	
	  public void deleteTweetsBlocking(RoutingContext routingContext) {
	    Map<String, Object> responseMap = new HashMap<String, Object>();
	    String response;
	    ObjectMapper mapper = new ObjectMapper();
	    String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription");
	    String date= (routingContext.request().getParam("date") == null) ? "": routingContext.request().getParam("date");
	   
	    
	    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	    Calendar cal = Calendar.getInstance();
	    cal.add(Calendar.DATE, -3);
	    String keyword=  format.format(cal.getTime()).toString();
	    if (date.isEmpty()) {
			date = keyword;
		}

	    try {
	    	
	      boolean success = false;
 
	      success = this.elasticsearch.deleteMaping(elasticsearch.getESInstance(esHost, esPort),indexEncription,date);
	     
	      if (success) {
	    	  responseMap.put("status", "true");
		}
	      else {
	    	  responseMap.put("status", "false");
		}
	      
	      response = mapper.writeValueAsString(responseMap);

	    } catch (Exception e) {
	    	log.error(e.getMessage());
	      response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
	          + "}";
	    }

	    routingContext.response().end(response);

	  }
	  
	  /**
		 * use to index documents in ES
		 * 
		 * @param tweets
	 * @throws Exception 
		 */
			public void indexInESearch(ArrayList<Map<String, Object>> tweets,String indexName,String type)throws Exception {

				TransportClient client = this.elasticsearch.getESInstance(this.esHost, this.esPort);
				BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
				for (Map<String, Object> tweet : tweets) {
						bulkRequestBuilder.add(client.prepareUpdate(indexName,type,tweet.get("id").toString()).setDoc(tweet).setUpsert(tweet));	
					}				
				bulkRequestBuilder.setRefresh(true).execute().actionGet();
	
				client.close();	
		
		}
	  
	  /**
	   * use to delete user index  
	   * 
	   * @param routingContext
	   */
	
	  public void deleteIndexBlocking(RoutingContext routingContext) {
	    Map<String, Object> responseMap = new HashMap<String, Object>();
	    String response;
	    ObjectMapper mapper = new ObjectMapper();
	    String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription");

	    try {
	      boolean success = false;
	      success = this.elasticsearch.deleteIndex(elasticsearch.getESInstance(esHost, esPort),indexEncription);
	      if (success) {
	    	  responseMap.put("status", "true");
		}
	      else {
	    	  responseMap.put("status", "false");
		}
	      
	      response = mapper.writeValueAsString(responseMap);

	    } catch (Exception e) {
	    	log.error(e.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

	      response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
	          + "}";
	    }

	    routingContext.response().end(response);

	  }
	
	  /**
	   * use to get index mappings 
	   * 
	   * @param routingContext
	   */
	
	  public void getMappingsBlocking(RoutingContext routingContext) {
	    Map<String, Object> responseMap = new HashMap<String, Object>();
	    String response;
	    ObjectMapper mapper = new ObjectMapper();
	    String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription");
	   
	    try {
	    	
	       	  String  Mappings = this.elasticsearch.getIndexMaping(elasticsearch.getESInstance(esHost, esPort),indexEncription);
	    	  responseMap.put("mappings", Mappings);
	
	      
	      response = mapper.writeValueAsString(responseMap);

	    } catch (Exception e) {
	    	log.error(e.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

	      response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
	          + "}";
	    }

	    routingContext.response().end(response);

	  }

	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void mutualRelationBlocking(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();
		String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription");
		String screenName = (routingContext.request().getParam("screenName") == null) ? "": routingContext.request().getParam("screenName");
		String id = (routingContext.request().getParam("id") == null) ? "tweet": routingContext.request().getParam("id");
		
		try {

			String getuserObject[] = {"userScreenName"};
			ArrayList<Object> commonRelation = new ArrayList<Object>();
			ArrayList<Object> nonCommonRelation = new ArrayList<Object>();
		
			ArrayList<Map<String, Object>> documents = this.elasticsearch.searchUserRelationDocuments(elasticsearch.getESInstance(esHost, esPort),indexEncription, getuserObject, screenName,0, documentsSize);
			if (documents.size() > 0 ) {
				commonRelation = (ArrayList<Object>) documents.get(0).get("commonRelation");
				nonCommonRelation = (ArrayList<Object>) documents.get(0).get("nonCommonFriends");
				LinkedList<ArrayList<Object>> esTotalFriends = new LinkedList<ArrayList<Object>>();
				esTotalFriends.add(commonRelation);
				esTotalFriends.add(nonCommonRelation);
				int check;
				boolean exist = false;
				for ( ArrayList<Object> esFollowers : esTotalFriends) {

						check = esFollowers.contains(id) ? 1 : 0;
						if ( check == 1 ) {
							
							exist =	true ;
						}
						
					}
				
				if ( exist == true) {
					
					responseMap.put("status","true");
				}
				else {
					responseMap.put("status","false");
				}
				
			}
			
			response = mapper.writeValueAsString(responseMap);

		} catch (Exception e) {
			log.error(e.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

			response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

	}
	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void stickyRelationBlocking(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();
		String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription");
		String screenName = (routingContext.request().getParam("screenName") == null) ? "": routingContext.request().getParam("screenName");
		String id = (routingContext.request().getParam("id") == null) ? "tweet": routingContext.request().getParam("id");
		
		try {

			String getuserObject[] = {"userScreenName"};
			ArrayList<Object> commonRelation = new ArrayList<Object>();
			ArrayList<Object> nonCommonRelation = new ArrayList<Object>();
		
			ArrayList<Map<String, Object>> documents = this.elasticsearch.searchUserRelationDocuments(elasticsearch.getESInstance(esHost, esPort),indexEncription, getuserObject, screenName,0, documentsSize);
			if (documents.size() > 0 ) {
				commonRelation = (ArrayList<Object>) documents.get(0).get("commonRelation");
				nonCommonRelation = (ArrayList<Object>) documents.get(0).get("nonCommonFollowers");
				LinkedList<ArrayList<Object>> esTotalFollowers = new LinkedList<ArrayList<Object>>();
				esTotalFollowers.add(commonRelation);
				esTotalFollowers.add(nonCommonRelation);
				int check;
				boolean exist = false;
				for ( ArrayList<Object> esFollowers : esTotalFollowers) {

						check = esFollowers.contains(id) ? 1 : 0;
						
						if ( check == 1 ) {
							exist =	true ;
						}
						
					}
				
				if (exist) {
					responseMap.put("status","true");
				}
				else {
					responseMap.put("status","false");
				}
							
			}
					
			response = mapper.writeValueAsString(responseMap);

		} catch (Exception e) {
			log.error(e.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

			response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

	}
	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void homeSearchBlocking(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal1 = Calendar.getInstance();
		Calendar cal2 = Calendar.getInstance();
	    cal1.add(Calendar.DATE, -1);
	    String oneDayPreviousDate=  formatter.format(cal1.getTime()).toString();
	    cal2.add(Calendar.DATE, -2);
	    String secondDayDate =  formatter.format(cal2.getTime()).toString();
		Date date = new Date();
		String types[] ={formatter.format(date),oneDayPreviousDate,secondDayDate};
		String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription");
		String size = (routingContext.request().getParam("size") == null) ? "500": routingContext.request().getParam("size");
		String page = (routingContext.request().getParam("page") == null) ? "0": routingContext.request().getParam("page");
		String sortOn = (routingContext.request().getParam("sort") == null) ? "time": routingContext.request().getParam("sort");
		String orderIn = (routingContext.request().getParam("order") == null) ? "": routingContext.request().getParam("order");

		int pageNo =0;
		SortOrder order = SortOrder.DESC;
		if (!orderIn.isEmpty() && orderIn.equalsIgnoreCase("0")) {
			order = SortOrder.ASC;
		}

		if (! size.isEmpty() || ! page.isEmpty()) {
			 pageNo = Integer.parseInt(page);
			this.documentsSize = Integer.parseInt(size);
		}
		try {	
			long start = System.currentTimeMillis();
			ArrayList<Map<String, Object>> documents = this.elasticsearch.homePageData(elasticsearch.getESInstance(esHost, esPort),indexEncription, types,pageNo, documentsSize,sortOn,order);
		if (documents.size() >= 1) {
			Map<String, Object> totalCount = documents.get(0);
			documents.remove(0);
			responseMap.put("size", totalCount.get("totalCount"));
		}
		long end = System.currentTimeMillis();
		System.err.println("time taken to get homepage data "+ (end-start));
		responseMap.put("status", "success");
		responseMap.put("documents", documents);
		response = mapper.writeValueAsString(responseMap);

		} catch (Exception e) {
			log.error(e.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

			response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

	}
	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void unfollowedFollowersBlocking(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();

		String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription");
		String type = (routingContext.request().getParam("type") == null) ? "unfollowedfollowers": routingContext.request().getParam("type");
		String size = (routingContext.request().getParam("size") == null) ? "500": routingContext.request().getParam("size");
		String page = (routingContext.request().getParam("page") == null) ? "0": routingContext.request().getParam("page");
		String sortOn = (routingContext.request().getParam("sort") == null) ? "time": routingContext.request().getParam("sort");
		String orderIn = (routingContext.request().getParam("order") == null) ? "": routingContext.request().getParam("order");

		int pageNo =0;
		SortOrder order = SortOrder.DESC;
		if (!orderIn.isEmpty() && orderIn.equalsIgnoreCase("0")) {
			order = SortOrder.ASC;
		}

		if (! size.isEmpty() || ! page.isEmpty()) {
			 pageNo = Integer.parseInt(page);
			this.documentsSize = Integer.parseInt(size);
		}
		try {	
			
			long start = System.currentTimeMillis();
			ArrayList<Map<String, Object>> documents = this.elasticsearch.getTypeData(elasticsearch.getESInstance(esHost, esPort),indexEncription, type,pageNo, documentsSize,sortOn,order);
		if (documents.size() >= 1) {
			Map<String, Object> totalCount = documents.get(0);
			documents.remove(0);
			responseMap.put("size", totalCount.get("totalCount"));
		}
		long end = System.currentTimeMillis();
		System.err.println("time taken to get unfollowed Followers data "+ (end-start));
		responseMap.put("status", "success");
		responseMap.put("documents", documents);
		response = mapper.writeValueAsString(responseMap);

		} catch (Exception e) {
			log.error(e.getMessage());
			response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

	}
	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void rangeFilterBlocking(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();
		
		String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription");
		String keyword = (routingContext.request().getParam("keyword") == null) ? "data": routingContext.request().getParam("keyword").toLowerCase();
		String searchIn = (routingContext.request().getParam("searchIn") == null) ? "[tweet]": routingContext.request().getParam("searchIn").toLowerCase();
		String size = (routingContext.request().getParam("size") == null) ? "100": routingContext.request().getParam("size");
		String page = (routingContext.request().getParam("page") == null) ? "0": routingContext.request().getParam("page");
		String field = (routingContext.request().getParam("fields") == null) ? "[]": routingContext.request().getParam("fields");
		String from = (routingContext.request().getParam("from") == null) ? "[]": routingContext.request().getParam("from");
		String to = (routingContext.request().getParam("to") == null) ? "[]": routingContext.request().getParam("to");
		String sortOn = (routingContext.request().getParam("sort") == null) ? "time": routingContext.request().getParam("sort");
		String orderIn = (routingContext.request().getParam("order") == null) ? "": routingContext.request().getParam("order");

		int pageNo = 0;
		SortOrder order = SortOrder.DESC;
		if (!orderIn.isEmpty() && orderIn.equalsIgnoreCase("0")) {
			order = SortOrder.ASC;
		}
		
		

		if (! size.isEmpty() || ! page.isEmpty()) {
			 pageNo = Integer.parseInt(page);
			this.documentsSize = Integer.parseInt(size);
		}
		try {	
			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			Calendar cal1 = Calendar.getInstance();
			Calendar cal2 = Calendar.getInstance();
		    cal1.add(Calendar.DATE, -1);
		    String oneDayPreviousDate=  formatter.format(cal1.getTime()).toString();
		    cal2.add(Calendar.DATE, -2);
		    String secondDayDate =  formatter.format(cal2.getTime()).toString();
			Date date = new Date();
			String types[] ={formatter.format(date),oneDayPreviousDate,secondDayDate};
			String[] fields = mapper.readValue(field, String[].class);
			String[] Searchingfields = mapper.readValue(searchIn, String[].class);
			String[] froms = mapper.readValue(from, String[].class);
			String[] tos = mapper.readValue(to, String[].class);
			ArrayList<Map<String, Object>> documents = this.elasticsearch.rangeFilter(elasticsearch.getESInstance(esHost, esPort),indexEncription,types,keyword,Searchingfields,fields,froms,tos,pageNo, documentsSize,sortOn,order);
		if (documents.size() >= 1) {
			Map<String, Object> totalCount = documents.get(0);
			documents.remove(0);
			responseMap.put("size",totalCount.get("totalCount"));
		}
		responseMap.put("status", "success");
		responseMap.put("documents", documents);
		response = mapper.writeValueAsString(responseMap);

		} catch (Exception e) {
			log.error(e.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

			response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

	}
	
	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void searchUserRelationBlocking(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();
		String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription").toLowerCase();
		String keyword = (routingContext.request().getParam("keyword") == null) ? "": routingContext.request().getParam("keyword").toLowerCase();
		String searchIn = (routingContext.request().getParam("searchIn") == null) ? "screenName": routingContext.request().getParam("searchIn");
		String page = (routingContext.request().getParam("page") == null) ? "0": routingContext.request().getParam("page");
		String size = (routingContext.request().getParam("size") == null) ? "1": routingContext.request().getParam("size");
		int pageNo = 0;
		int pageSize = 1;
		if (!page.isEmpty() && ! size.isEmpty()) {
			 pageNo = Integer.parseInt(page);
			 pageSize = Integer.parseInt(size);
		}
		try {
			String[] fields = mapper.readValue(searchIn, String[].class);
			ArrayList<Map<String, Object>> documents = this.elasticsearch.searchUserRelationDocuments(this.elasticsearch.getESInstance(this.esHost, this.esPort),indexEncription, fields, keyword,pageNo,pageSize);
			responseMap.put("status", "success");
			responseMap.put("documents", documents);
			response = mapper.writeValueAsString(responseMap);

		} catch (Exception e) {
			log.error(e.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

			response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

	}
	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void searchUserBlocking(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();
		String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription").toLowerCase();
		String searchIn = (routingContext.request().getParam("searchIn") == null) ? "commonrelation": routingContext.request().getParam("searchIn").toLowerCase();
		String page = (routingContext.request().getParam("page") == null) ? "0": routingContext.request().getParam("page");
		String size = (routingContext.request().getParam("size") == null) ? "100": routingContext.request().getParam("size");
		String bit = (routingContext.request().getParam("bit") == null) ? "0": routingContext.request().getParam("bit");
		String sortOn = (routingContext.request().getParam("sort") == null) ? "time": routingContext.request().getParam("sort");
		String orderIn = (routingContext.request().getParam("order") == null) ? "": routingContext.request().getParam("order");

		int pageNo = 0;
		SortOrder order = SortOrder.DESC;
		if (!orderIn.isEmpty() && orderIn.equalsIgnoreCase("0")) {
			order = SortOrder.ASC;
		}
		
		int	flagbit = 0 ;
		int documentSize = 0;
		
		if (!(page.isEmpty()) || !(bit.isEmpty()) && !(size.isEmpty()) ) {
			 pageNo = Integer.parseInt(page);
			flagbit =Integer.parseInt(bit);
			documentSize = Integer.parseInt(size);
		}
		
		try {
			long start = System.currentTimeMillis();
			String nonCommonRelation = "noncommon"+searchIn;
			ArrayList<Map<String, Object>> commonDocuments =null;
			ArrayList<Map<String, Object>> nonCommonDocuments =null;
		
			if (flagbit==0) {
			
				nonCommonDocuments = this.elasticsearch.getTypeData(elasticsearch.getESInstance(esHost, esPort),indexEncription,nonCommonRelation,pageNo,documentSize,sortOn,order);
				if (nonCommonDocuments.size() >= 1) {
					responseMap.put("totalCount",nonCommonDocuments.get(0).get("totalCount"));
					nonCommonDocuments.remove(0);
				}
				responseMap.put("status", "success");
				responseMap.put("nonCommonDocuments", nonCommonDocuments);
				
			} else {
				searchIn = "commonrelation";
				commonDocuments = this.elasticsearch.getTypeData(this.elasticsearch.getESInstance(this.esHost,this.esPort),indexEncription, searchIn,pageNo,documentSize,sortOn,order);
				if (commonDocuments.size() >= 1) {
					responseMap.put("totalCount",commonDocuments.get(0).get("totalCount"));
					commonDocuments.remove(0);
				}
				responseMap.put("status", "success");
				responseMap.put("commonDocuments", commonDocuments);
			}

			long end = System.currentTimeMillis()-start;
			System.out.println("time taken "+end);
			response = mapper.writeValueAsString(responseMap);

		} catch (Exception e) {
			log.error(e.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

			response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

	}
	
	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void searchUserInfluenceBlocking(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();
		String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription").toLowerCase();
		String userScreenName = (routingContext.request().getParam("userScreenName") == null) ? "": routingContext.request().getParam("userScreenName").toLowerCase();
		String influenceScreenName = (routingContext.request().getParam("influenceScreenName") == null) ? "followers": routingContext.request().getParam("influenceScreenName").toLowerCase();
		String page = (routingContext.request().getParam("page") == null) ? "0": routingContext.request().getParam("page");
		String size = (routingContext.request().getParam("size") == null) ? "100": routingContext.request().getParam("size");
		String bit = (routingContext.request().getParam("bit") == null) ? "0": routingContext.request().getParam("bit");
		String sortOn = (routingContext.request().getParam("sort") == null) ? "time": routingContext.request().getParam("sort");
		String orderIn = (routingContext.request().getParam("order") == null) ? "": routingContext.request().getParam("order");

		int pageNo = 0;
		int documentSize = 100;
		SortOrder order = SortOrder.DESC;
		if (!orderIn.isEmpty() && orderIn.equalsIgnoreCase("0")) {
			order = SortOrder.ASC;
		}
		int	flagBit = 0;
		if (!(page.isEmpty()) || !(bit.isEmpty()) || !size.isEmpty()) {
			 pageNo = Integer.parseInt(page);
			flagBit = Integer.parseInt(bit);
			documentSize = Integer.parseInt(size);
		}

		try {

			if (flagBit ==0) {

			ArrayList<Map<String, Object>>	documents = this.elasticsearch.getTypeData(elasticsearch.getESInstance(esHost, esPort),indexEncription,userScreenName+influenceScreenName+"noncommonfollowers",pageNo,documentSize,sortOn,order);
			if (documents.size() >= 1) {
				responseMap.put("totalCount",documents.get(0).get("totalCount"));
				documents.remove(0);
			}
				responseMap.put("status", "success");
				responseMap.put("documents", documents);
			} else {
				
	
				ArrayList<Map<String, Object>>	documents = this.elasticsearch.getTypeData(elasticsearch.getESInstance(esHost, esPort),indexEncription,userScreenName+influenceScreenName+"commonfollowers",pageNo,documentSize,sortOn,order);
				if (documents.size() >= 1) {
					responseMap.put("totalCount",documents.get(0).get("totalCount"));
					documents.remove(0);
				}
				responseMap.put("status", "success");
				responseMap.put("documents", documents);
			}
			
			
			
			response = mapper.writeValueAsString(responseMap);

		} catch (Exception e) {
			log.error(e);
			response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

	}
	
	
	 public String[] getIds(ArrayList<Object> ids){
		  String [] id =new String[ids.size()];
		  for (int i=0;i< ids.size();i++) {
			  
			id[i] = ids.get(i).toString();
		}
		  
		  return id;
	  }
	 
	 public String[] getArrayIds(ArrayList<String> ids){
		  String [] id =new String[ids.size()];
		  for (int i=0;i< ids.size();i++) {
			  
			id[i] = ids.get(i).toString();
		}
		  
		  return id;
	  }
	  
	  public  LinkedList<String[]> chunksIds(String [] bigList, int n) {
			int partitionSize = n;
			LinkedList<String[]> partitions = new LinkedList<String[]>();
			for (int i = 0; i < bigList.length; i += partitionSize) {
				String[] bulk = Arrays.copyOfRange(bigList, i,
						Math.min(i + partitionSize, bigList.length));
				partitions.add(bulk);
			}

			return partitions;
		}
	
}
