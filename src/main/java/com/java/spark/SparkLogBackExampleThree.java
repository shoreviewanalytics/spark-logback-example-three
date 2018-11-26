package com.java.spark;


import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.spark.SparkConf;
import com.datastax.bdp.fs.hadoop.*;

import org.slf4j.LoggerFactory;

import org.slf4j.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;


@SuppressWarnings("unused")
public class SparkLogBackExampleThree {
	
	final static Logger logger = LoggerFactory.getLogger(SparkLogBackExampleThree.class);
	private static final String rawData = null;
	
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
			
			DseFileSystem fileSystem = (DseFileSystem) DseFileSystem.get(hdfsconf);
			
			Path home = fileSystem.getHomeDirectory();  // home dir for user 
			
			
						
			logger.info(home.toString());
						
						
			boolean exists = fileSystem.exists(new Path("dsefs://10.1.10.51:5598/jobs/sle3/logback.xml"));
			
			boolean isfile = fileSystem.isFile(new Path("dsefs://10.1.10.51:5598/jobs/sle3/logback.xml"));
			
			
			// change permission  
			//fileSystem.setPermission(new Path("file:///jobs/sle3/logback.xml"), new FsPermission((short)0777));
			
			// change owner
			//fileSystem.setOwner(new Path("file:///jobs/sle3/logback.xml"), "one", "one");
			
					
			if (exists == true) {

				//FSDataInputStream inputStream = fileSystem.open(new Path("dsefs://10.1.10.51:5598/jobs/sle3/logback.xml"));
				//InputStream inputStream = fileSystem.open(new Path("dsefs://10.1.10.51:5598/jobs/sle3/logback.xml"));
				//InputStream inputStream = fileSystem.open(new Path("file:///jobs/sle3/logback.xml"));
				
				InputStream inputStream = fileSystem.open(new Path("dsefs://10.1.10.51:5598/jobs/sle3/logback.xml"));
								
							
	/*			ObjectInputStream configStream = new ObjectInputStream(inputStream);*/
				
				BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
				
				byte[] bs = new byte[inputStream.available()];
				 
				
				/*try {

					JoranConfigurator configurator = new JoranConfigurator();
					configurator.setContext(context);
					context.reset();
					configurator.doConfigure(inputStream);
				} catch (JoranException je) {

				}

				StatusPrinter.printInCaseOfErrorsOrWarnings(context);*/
				
				try {
				
					String line;
					String text;
					text = br.toString();
					line = br.readLine();
					
					while (line != null) {
						logger.info(line);
					   
						line = br.readLine();
				    }
				}	finally {
						br.close();
						inputStream.close();
						fileSystem.close();
						
					}
				
				
				
				}
				
			
			
			// }catch(IOException | URISyntaxException | IllegalArgumentException e){
		} catch (IOException | IllegalArgumentException e) {

			logger.error("An Error has occurred: " + e.getMessage());

		}
	    	
		sc.close();
		 

	}

}
