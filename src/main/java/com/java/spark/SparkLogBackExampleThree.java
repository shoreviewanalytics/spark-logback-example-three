package com.java.spark;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import org.slf4j.LoggerFactory;

import org.slf4j.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;

@SuppressWarnings("unused")
public class SparkLogBackExampleThree {

	final static Logger logger = LoggerFactory.getLogger(SparkLogBackExampleThree.class);
	private static boolean isLocal = true;

	public static void main(String[] args) {

		try {

			System.setProperty("jobname", "spark-logback-example-three");
			if (isLocal == true) {
				SparkConf conf = new SparkConf().setAppName("SparkLogBackExampleThree").setMaster("local[1]");
				@SuppressWarnings("resource")
				JavaSparkContext sc = new JavaSparkContext(conf);
			} else {

				SparkConf conf = new SparkConf().setAppName("SparkLogBackExampleThree");
				@SuppressWarnings("resource")
				JavaSparkContext sc = new JavaSparkContext(conf);
			}

			LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();

			Configuration hdfsconf = new Configuration();

			hdfsconf.set("fs.defaultFS", "dsefs://10.1.10.52");
			hdfsconf.set("fs.dsefs.impl", "com.datastax.bdp.fs.hadoop.DseFileSystem");
			hdfsconf.set("com.datastax.bdp.fs.client.authentication.factory",
					"com.datastax.bdp.fs.hadoop.DseRestClientAuthProviderBuilderFactory");
			hdfsconf.set("com.datastax.bdp.fs.client.authentication.basic.username", "cassandra");
			hdfsconf.set("com.datastax.bdp.fs.client.authentication.basic.password", "");

			FileSystem fileSystem = FileSystem.get(new URI("dsefs://10.1.10.52:5598"), hdfsconf);
			boolean exists = fileSystem.exists(new Path("dsefs://10.1.10.52:5598/jobs/sle3/logback.xml"));

			String filePath = "dsefs://10.1.10.52:5598/jobs/sle3/logback.xml";
			Path fpath = new Path(filePath);

			if (exists == true) {

				fileSystem = fpath.getFileSystem(hdfsconf);

				FSDataInputStream inputStream = fileSystem.open(fpath);

				try {
					JoranConfigurator configurator = new JoranConfigurator();
					configurator.setContext(context);
					context.reset();
					configurator.doConfigure(inputStream);
				} catch (JoranException je) {

				}

				StatusPrinter.printInCaseOfErrorsOrWarnings(context);

			}

			logger.info("job start");

			logger.info("job end");

		} catch (IOException | IllegalArgumentException | URISyntaxException e) {

			logger.error("A fatal error has occurred: " + e.getMessage());

		}

	}

}
