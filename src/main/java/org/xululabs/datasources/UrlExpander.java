package org.xululabs.datasources;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;

/**
 * @author Abhijit Ghosh
 * @version 1.0
 */
public class UrlExpander {

	public static void main(String[] args) throws IOException {
		String shortenedUrl = "https://t.co/pxdYP2k0z5";
		
		    System.err.println("new "+getLongUrl(shortenedUrl));

		
		System.out.println("size "+shortenedUrl.length());
		String expandedURL = expandUrl(shortenedUrl);
		
		System.out.println(shortenedUrl + "-->\n" + expandedURL); 
	}
	
	public static String getLongUrl(String shortUrl) throws IOException{
	    
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
	
	public static String expandUrl(String shortenedUrl) throws IOException {
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
}
