package org.xululabs.javaservices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.vertx.core.Vertx;



public class MainJavaServices {
	
	private static Logger log = LogManager.getRootLogger();

	public static void main(String[] args) {
		log.info("Initializing twittertool-v3");
		
		Vertx vertx = Vertx.vertx();
		
//		org.xululabs.twittertool_v3_airflow.Main
//		org.xululabs.javaservices.MainJavaServices
		
		
		vertx.deployVerticle(new SearchingServer());
//		vertx.deployVerticle(new IndexingServer());

	}

}
