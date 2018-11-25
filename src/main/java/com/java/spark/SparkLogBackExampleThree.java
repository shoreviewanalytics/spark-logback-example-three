package com.java.spark;


import org.apache.spark.api.java.JavaSparkContext;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;


public class SparkLogBackExampleThree {
	
	final static Logger logger = LoggerFactory.getLogger(SparkLogBackExampleThree.class);
	
	public static void main(String[] args) {
		
		System.setProperty("jobname","app2");
		
		SparkConf conf = new SparkConf()
				.setAppName("SparkLogBackExampleThree")
				.setMaster("local[1]") // comment out if not running locally.
				;
				
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
        
		
		LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
				
		try {
			
			Configuration hdfsconf = new Configuration();
			
			
			hdfsconf.set("fs.dsefs.impl", "com.datastax.bdp.fs.hadoop.DseFileSystem");
			hdfsconf.set("com.datastax.bdp.fs.client.authentication.basic.username", "cassandra");
			hdfsconf.set("com.datastax.bdp.fs.client.authentication.basic.password", "");
			hdfsconf.set("com.datastax.bdp.fs.client.authentication.factory", "com.datastax.bdp.fs.hadoop.RestClientAuthProviderBuilderFactory");
			
			
		    FileSystem fileSystem = FileSystem.get(new URI("dsefs://10.1.10.51:5598"),hdfsconf);		    
			
			FSDataInputStream inputStream = fileSystem.open(new Path("dsefs://10.1.10.51:5598/jobs/sle3/logback.xml"));
			
			ObjectInputStream configStream = new ObjectInputStream(inputStream);			
			
			try {
				
			      JoranConfigurator configurator = new JoranConfigurator();
			      configurator.setContext(context);
			      context.reset(); 
			      configurator.doConfigure(configStream);
			    } catch (JoranException je) {
			      
			    }
			
				StatusPrinter.printInCaseOfErrorsOrWarnings(context);
				
		}catch(IOException | URISyntaxException | IllegalArgumentException e){
			
			logger.error("An Error has occurred: " + e.getMessage());	
			 
			
		}	
	    	
		sc.close();

	}

}
