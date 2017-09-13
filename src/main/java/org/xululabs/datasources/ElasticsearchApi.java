package org.xululabs.datasources;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

public class ElasticsearchApi {
	private static Logger log = LogManager.getRootLogger();
	/**
	 * use to get elasticsearch instance
	 * 
	 * @param host
	 * @param port
	 * @return client
	 */
	 public TransportClient getESInstance(String host, int port)
			throws Exception {
		 Settings settings = ImmutableSettings.settingsBuilder()
			        .put("cluster.name", "elasticsearchairflow").build();
			TransportClient client = new TransportClient(settings)
			.addTransportAddress(new InetSocketTransportAddress(host, port));
			
			return client;
		
		
	}
	 
	 /**
		 * use to delete elasticSearch mapping older than 3 days 
		 * 
		 * @param client
		 * @param indexname
		 * @return typeName
		 */

	public  boolean deleteMaping(TransportClient client,String index,String type) throws Exception {
		
		boolean deleted = false;
		try {
			
		
		// getting all types of a index then delete older than 3 days
		ClusterStateResponse resp = client.admin().cluster().prepareState().execute().actionGet(); 
		
		ImmutableOpenMap<String,MappingMetaData> mappings = resp.getState().metaData().index(index).mappings(); 
		System.err.println(index+" - "+mappings.keys().size()+" - "+mappings.keys());
		String mapping = mappings.keys().toString();
		String mappingParts[] = mapping.substring(1, mapping.length()-1).split(",");
		
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		
		for (int i = 0; i < mappingParts.length; i++) {
			

			if (!mappingParts[i].contains("-")) {
				continue;
			}

			Date date1 = format.parse(mappingParts[i]);
			Date date2 = format.parse(type);
			
			if (date1.compareTo(date2) == 0 ||  date1.compareTo(date2) < 0 ) {
				String deletDate = mappingParts[i].trim();
				
				BoolQueryBuilder boolQuery = new BoolQueryBuilder();
				boolQuery.should(QueryBuilders.matchPhraseQuery("date", deletDate));
				
				DeleteByQueryResponse response1 = client.prepareDeleteByQuery().setIndices(index).setTypes(deletDate)
				        .setQuery(boolQuery)
				        .execute()
				        .actionGet();
		DeleteMappingResponse response = client.admin().indices().prepareDeleteMapping(index).setType(deletDate).execute().actionGet(); 
		System.out.println("deleted !"+response.isAcknowledged());
		deleted = response.isAcknowledged();
				}
		
			}

		} catch (Exception e) {
				System.err.println(e);
			 }
				
				
				client.close();
				return deleted;
	}
	
	 /**
		 * use to delete elasticSearch mapping older than 3 days 
		 * 
		 * @param client
		 * @param indexname
		 * @return typeName
		 */

	public  boolean deleteOnlyMaping(TransportClient client,String index,String type) throws Exception {
		
		boolean deleted = false;
		try {
			
		
		// getting all types of a index then delete older than 3 days
		ClusterStateResponse resp = client.admin().cluster().prepareState().execute().actionGet(); 
		
		ImmutableOpenMap<String,MappingMetaData> mappings = resp.getState().metaData().index(index).mappings(); 
		if (mappings.containsKey(type)) { 
			DeleteMappingResponse response = client.admin().indices().prepareDeleteMapping(index).setType(type).execute().actionGet(); 
			System.out.println("deleted !"+response.isAcknowledged());
			deleted = response.isAcknowledged();
		} 
				} catch (Exception e) {
					log.error(e.getMessage());
//					log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

						}
				
				
				client.close();
				return deleted;
	}
	 
	 /**
	 * use to delete elasticSearch mapping older than 3 days 
	 * 
	 * @param client
	 * @param indexname
	 * @return typeName
	 */

	public  String getIndexMaping(TransportClient client,String index) throws Exception {
	
		String types = null;
	try {
	// getting all types of a index then delete older than 3 days
	ClusterStateResponse resp = client.admin().cluster().prepareState().execute().actionGet(); 
	
	ImmutableOpenMap<String,MappingMetaData> mappings = resp.getState().metaData().index(index).mappings(); 
	 types = mappings.keys().toString();
			
		} catch (Exception e) {
			log.error(e.getMessage());
			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

		}
			client.close();
			return types;
}
	
	public ArrayList<Map<String, Object>> searchDocuments(TransportClient client,String indexName, String field,String keywords[],int page, int documentsSize) throws Exception {
		
		ArrayList<Map<String, Object>> documents = new ArrayList<Map<String, Object>>();
		try {
			BoolQueryBuilder boolQuery = new BoolQueryBuilder();
			for (String keyword : keywords) {
				boolQuery.should(QueryBuilders.matchPhraseQuery(field, keyword));
			}
			SearchResponse response = client.prepareSearch(indexName)
					.setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(boolQuery)
					.setFrom(page).setSize(documentsSize).setExplain(true).execute()
					.actionGet();
			SearchHit[] results = response.getHits().getHits();
			Map<String, Object> totalCount = new HashMap<String, Object>();
			totalCount.put("totalCount", response.getHits().getTotalHits());

			documents.add(totalCount);
			for (SearchHit hit : results) {
				
				Map<String, Object> result = hit.getSource(); // the retrieved document	
				documents.add(result);
			}
		} catch (Exception e) {
			log.error(e.getMessage());
			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

		}
		
		// close client
		client.close();
		return documents;
	}
	
	public boolean deleteIndex(TransportClient client,String indexName){

		boolean isexist = indexExist(client, indexName);
	boolean	deleted = false;
		DeleteIndexResponse response = null;
		try {

		if (isexist) {
			 DeleteIndexRequest request = new DeleteIndexRequest(indexName);
			  response = client.admin().indices().delete(request).actionGet();// dell only index
			 
			 // dell mapping e.g (types)
//			 DeleteMappingResponse response = client.admin().indices().prepareDeleteMapping("e72c504dc16c8fcd2fe8c74bb492affa").setType("2017-03-29").execute().actionGet();
			 System.out.println("deleted !"+response.isAcknowledged());
			 deleted = response.isAcknowledged();
		}
			} catch (Exception e) {
				log.error(e.getMessage());
				log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

					}
		
			return deleted; 
	}
	
	
	public ArrayList<Map<String, Object>> searchTweetDocuments(TransportClient client,String indexName,String types[], String fields[],String keyword,int page, int documentsSize,String sortOn,SortOrder order) throws Exception {
		
		ArrayList<Map<String, Object>> documents = new ArrayList<Map<String, Object>>();
		try {
	
		BoolQueryBuilder boolQuery = new BoolQueryBuilder();
		for (String field : fields) {
			boolQuery.should(QueryBuilders.matchPhraseQuery(field, keyword));
		}
		SearchResponse response = client.prepareSearch(indexName).setTypes(types[0],types[1],types[2])
				.setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(boolQuery).addSort(sortOn,order)
				.setFrom(page).setSize(documentsSize).setExplain(true).execute()
				.actionGet();
		SearchHit[] results = response.getHits().getHits();
		Map<String, Object> totalCount = new HashMap<String, Object>();
		totalCount.put("totalCount", response.getHits().getTotalHits());

		documents.add(totalCount);
		for (SearchHit hit : results) {
			
			Map<String, Object> result = hit.getSource(); // the retrieved document
			/*if (result.get("externalUrl").toString()=="") {
				result.put("externalUrl", "N/A");
			}
			else {

					result.put("externalUrl", this.expandUrl(result.get("externalUrl").toString()));		
			}*/
			documents.add(result);
			}
		} catch (Exception e) {
			log.error(e.getMessage());
			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

		}
		// close client
		client.close();
		return documents;
	}
	
public  String getLongUrl(String shortUrl) throws IOException{
	    
	    String result=shortUrl;
	    String header;
	    do {
	        URL url=new URL(result);
	        HttpURLConnection .setFollowRedirects(false);
	        URLConnection conn=url.openConnection();
	        header=conn.getHeaderField(null);
	        String location=conn.getHeaderField("location");
	        if(location!=null){
	          result = location;
	        }
	    }
	    
	        while (header.contains("301"));
	    
	    
	      return result;
	    }
	
	public  String expandUrl(String shortenedUrl) throws IOException {
		URL url = new URL(shortenedUrl);	
		// open connection
        HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY); 
       
        // stop following browser redirect
        httpURLConnection.setInstanceFollowRedirects(false);
        
        // extract location header containing the actual destination URL
        String expandedURL = httpURLConnection.getHeaderField("Location");
        httpURLConnection.disconnect();
        if (expandedURL==null) {
        	expandedURL = shortenedUrl;
		}
        return expandedURL;
	}

	public ArrayList<Map<String, Object>> searchUserRelationDocuments(TransportClient client,String indexName, String fields[],String keyword,int page, int documentsSize) throws Exception {
		
		ArrayList<Map<String, Object>> documents = new ArrayList<Map<String, Object>>();
		try {
			
		
		BoolQueryBuilder boolQuery = new BoolQueryBuilder();
		for (String field : fields) {
			boolQuery.should(QueryBuilders.matchPhraseQuery(field, keyword));
		}
		SearchResponse response = client.prepareSearch(indexName)
				.setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(boolQuery).addSort("date",SortOrder.DESC)
				.setFrom(page).setSize(documentsSize).setExplain(true).execute()
				                                                                                                                                                                                                                                                                                                                                                                .actionGet();
		SearchHit[] results = response.getHits().getHits();

		for (SearchHit hit : results) {
			
			Map<String, Object> result = hit.getSource(); // the retrieved document
			documents.add(result);
		}
			} catch (Exception e) {
				log.error(e.getMessage());
				log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

		}
		// close client
		client.close();
		return documents;
	}
	
	// GENERAL FUNCTION TO get any type data SEARCH
			public ArrayList<Map<String, Object>> getTypeData(TransportClient client,String index, String type,int page,int size,String sortOn,SortOrder order) {
				ArrayList<Map<String, Object>> documents = new ArrayList<Map<String, Object>>();
				try {
					
				SearchResponse response = client.prepareSearch(index).setTypes(type)
						.setSearchType(SearchType.QUERY_THEN_FETCH).addSort(sortOn,order).setFrom(page)
						.setSize(size)
						.execute().actionGet();
				Map<String, Object> totalCount = new HashMap<String, Object>();
				totalCount.put("totalCount", response.getHits().getTotalHits());
				documents.add(totalCount);
				for (SearchHit hit : response.getHits()) {
					Map<String, Object> result = hit.getSource(); // the retrieved document
					documents.add(result);
				}
				
					} catch (Exception e) {
						log.error(e.getMessage());
						log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

					}
				client.close();
				return documents;
			}

	// GENERAL FUNCTION TO SEARCH
		public ArrayList<Map<String, Object>> homePageData(TransportClient client,String index, String types[],int page,int size,String sortOn,SortOrder order) {
			ArrayList<Map<String, Object>> documents = new ArrayList<Map<String, Object>>();
		
			try {

			SearchResponse response = client.prepareSearch(index).setTypes(types[0],types[1],types[2])
					.setSearchType(SearchType.QUERY_THEN_FETCH).addSort(sortOn,order).setFrom(page)
					.setSize(size)
					.execute().actionGet();
			Map<String, Object> totalCount = new HashMap<String, Object>();
			totalCount.put("totalCount", response.getHits().getTotalHits());
			documents.add(totalCount);
			for (SearchHit hit : response.getHits()) {
				Map<String, Object> result = hit.getSource(); // the retrieved document
				/*if (result.get("externalUrl").toString()=="") {
					result.put("externalUrl", "N/A");
				}
				else {
					
					result.put("externalUrl", this.expandUrl(result.get("externalUrl").toString()));
				}*/
				documents.add(result);
			}
				} catch (Exception e) {
					log.error(e.getMessage());
					log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

				}
			client.close();
			return documents;
		}
		
		// Search using Range QUERY
		public ArrayList<Map<String, Object>>  rangeFilter(TransportClient client,String index,String types[],String keyword,String searchIn[],String fields[], String from[],String to[],int page,int documentsSize,String sortOn,SortOrder order) {
			ArrayList<Map<String, Object>> documents = new ArrayList<Map<String, Object>>();
			try {

			BoolQueryBuilder boolquery = new BoolQueryBuilder();
			BoolFilterBuilder boolFilterBuilder = new BoolFilterBuilder();
			
			for (int j = 0; j < searchIn.length; j++) {
				if (searchIn[j] == "" || keyword == "") {
					break;
				}
				boolquery.should(QueryBuilders.matchPhraseQuery(searchIn[j],keyword));
			}
			
			for (int i = 0 ; i<fields.length ; i++) {
				
				if (from[i].equals("")) {
					from[i] = "0";
				}
				if (to[i].equals("") ) {
					to[i] = "50000000000";
				}
				
				boolFilterBuilder.must(FilterBuilders.rangeFilter(fields[i]).gte(from[i]).lte(to[i]));
			}

			SearchResponse response = client.prepareSearch(index).setTypes(types[0],types[1],types[2])
					.setSearchType(SearchType.QUERY_THEN_FETCH)
					.setQuery(QueryBuilders.filteredQuery(boolquery,boolFilterBuilder))
					.addSort(sortOn,order)
					.setFrom(page).setSize(documentsSize).setExplain(true).execute()
					.actionGet();
			
			SearchHit[] results = response.getHits().getHits();
		
			Map<String, Object> totalSize = new HashMap<String, Object>();
			totalSize.put("totalCount", response.getHits().getTotalHits());
			documents.add(totalSize);
			for (SearchHit hit : results) {

				Map<String, Object> result = hit.getSource(); // the retrieved document
				documents.add(result);
			}
				} catch (Exception e) {
					log.error(e.getMessage());
					log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

				}
			
			// close client
			client.close();
			return documents;
			
			
			}
	
	
	public boolean documentsExist(TransportClient client, String index, String type,String[] ids) throws Exception {
		boolean success = false;
		try {
			
		
		MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
			    .add(index, type, ids)           
			    .get();

			for (MultiGetItemResponse itemResponse : multiGetItemResponses) { 
			    GetResponse response = itemResponse.getResponse();
			    if (response.isExists()) {                      
			       success = true;
			    }
			}
				} catch (Exception e) {
					log.error(e.getMessage());
					log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

				}
			client.close();
			return success;
			
	}
	
		public ArrayList<Map<String, Object>> searchUserDocumentsByIds(TransportClient client, String index, String type,String[] ids) throws Exception {
			
			ArrayList<Map<String, Object>> documents = new ArrayList<Map<String, Object>>();
			try {
				
		
			MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
				    .add(index,type, ids)           
				    .get();
			
				for (MultiGetItemResponse itemResponse : multiGetItemResponses) { 
				    GetResponse response = itemResponse.getResponse();
				    if (response.isExists()) {                      
				        Map<String, Object> map = response.getSourceAsMap(); 
				      
				        documents.add(map);
				    }
				}
			} catch (Exception e) {
				log.error(e.getMessage());
//				log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

			}
			// close client
			client.close();
			return documents;
		}
		
	
	public  boolean indexExist(TransportClient client,String INDEX_NAME) {
		IndexMetaData indexMetaData = null;
		try {
			
		
		 indexMetaData = client.admin().cluster().state(Requests.clusterStateRequest())
	            .actionGet()
	            .getState()
	            .getMetaData()
	            .index(INDEX_NAME);
				} catch (Exception e) {
					
				}
	    return (indexMetaData != null);	
	   
	}


}
