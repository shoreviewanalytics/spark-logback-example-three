package com.java.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

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


import scala.Tuple2;
import java.util.regex.Pattern;

public class SparkLogBackExampleThree {
	
	final static Logger logger = LoggerFactory.getLogger(SparkLogBackExampleThree.class);
	
	public static void main(String[] args) {
		
		System.setProperty("jobname","app2");
		
		final Pattern SPACE = Pattern.compile(" ");

		SparkConf conf = new SparkConf().setAppName("SparkLogBackExampleThree");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession spark = SparkSession.builder().appName("SparkLogBackExampleThree").getOrCreate();
		
				
		try {
			
			LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
			
			Configuration hdfsconf = new Configuration();
			hdfsconf.set("fs.dsefs.impl", "com.datastax.bdp.fs.hadoop.DseFileSystem");
			hdfsconf.set("com.datastax.bdp.fs.client.authentication.basic.username", "cassandra");
			hdfsconf.set("com.datastax.bdp.fs.client.authentication.basic.password", "");
		    
					
			FileSystem fileSystem;
			try {
				fileSystem = FileSystem.get(new URI("dsefs://10.1.10.51:5598/"),hdfsconf);
				
				String file = "jobs/sle3/logback.xml";

			    Path path = new Path(file);
			    if (!fileSystem.exists(path)) {
			      logger.info("File " + file + " does not exist.");
			      return;
			    }

			    FSDataInputStream logbackPropertiesUserFile = fileSystem.open(path);	
			    
			    try {
					
				      JoranConfigurator configurator = new JoranConfigurator();
				      configurator.setContext(context);
				      context.reset(); 
				      configurator.doConfigure(logbackPropertiesUserFile);
				    } catch (JoranException je) {
				      
				    }
				
					StatusPrinter.printInCaseOfErrorsOrWarnings(context);
			} catch (URISyntaxException e) {
				
				e.printStackTrace();
			}
			
				
		    						
			
				
		}catch(Exception e){
			
			logger.error("An Error has occurred: " + e.getMessage());		
			 
			
		}
		
	    
	
	
	    
	    
	    
		
		
		SysStreamsLogger.bindSystemStreams();

		// sample info logging

		logger.info("Sample Info Logging: Starting up the job.");

		/**
		 * to test against a distributed environment place a file named words.csv on each node.
		 * 
		 */
		JavaRDD<String> lines = spark.read().textFile("file:///home/one/data/words.csv").javaRDD();

		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

		logger.info("Sample Info Logging of transformation of words after lines.flatMap.");

		words.take(5).forEach(System.out::println);

		JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

		logger.info("Sample Info Logging of transformation of ones RDD after being set to words.mapToPair.");

		ones.take(5).forEach(System.out::println);

		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

		logger.info("Sample Info Logging of the RDD - counts.toDebugString() " + counts.toDebugString());

		// sample info logging of transformation using SysStreamsLogger
		logger.info("Sample Info Logging: Print out the first five key / value pairs of counts RDD");

		counts.take(5).forEach(System.out::println);

		// sample warn logging

		logger.warn("Sample Warning Logging: Exiting the job.");

		// sample error logging

		logger.error("Sample Error Logging: There's an error to address!");

		sc.close();

	}

}
