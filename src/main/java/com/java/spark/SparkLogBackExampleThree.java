package com.java.spark;


import org.apache.spark.api.java.JavaSparkContext;
import java.io.IOException;
import java.io.ObjectInputStream;

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
				.setMaster("local[1]") // comment when not running locally.
				;
				
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
        
		
		LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
				
		try {
			
			Configuration hdfsconf = new Configuration();
			
			hdfsconf.set("fs.defaultFS", "dsefs://10.1.10.51");
			hdfsconf.set("fs.dsefs.impl", "com.datastax.bdp.fs.hadoop.DseFileSystem");
			hdfsconf.set("com.datastax.bdp.fs.client.authentication.factory", "com.datastax.bdp.fs.hadoop.DseRestClientAuthProviderBuilderFactory");
			hdfsconf.set("com.datastax.bdp.fs.client.authentication.basic.username", "cassandra");
			hdfsconf.set("com.datastax.bdp.fs.client.authentication.basic.password", "");
			
			
		    //FileSystem fileSystem = FileSystem.get(new URI("dsefs://10.1.10.51:5598"),hdfsconf);	
			
			FileSystem fileSystem = FileSystem.get(hdfsconf);
			
			Path home = fileSystem.getHomeDirectory();  // this is the home directory for user running the job. 
						
			logger.info(home.toString());
						
			//FSDataInputStream inputStream = fileSystem.open(new Path("dsefs://10.1.10.51:5598/jobs/sle3/logback.xml"));
			
			boolean exists = fileSystem.exists(new Path("file:///jobs/sle3/logback.xml"));
			
			if (exists == true) {

				FSDataInputStream inputStream = fileSystem.open(new Path("file:///jobs/sle3/logback.xml"));
				
				logger.info(inputStream.toString());

				byte[] bs = new byte[inputStream.available()];
				inputStream.readFully(bs);

				ObjectInputStream configStream = new ObjectInputStream(inputStream);

				try {

					JoranConfigurator configurator = new JoranConfigurator();
					configurator.setContext(context);
					context.reset();
					configurator.doConfigure(configStream);
				} catch (JoranException je) {

				}

				StatusPrinter.printInCaseOfErrorsOrWarnings(context);

			}

			// }catch(IOException | URISyntaxException | IllegalArgumentException e){
		} catch (IOException | IllegalArgumentException e) {

			logger.error("An Error has occurred: " + e.getMessage());

		}
	    	
		sc.close();

	}

}
