/****************************************************************************

                   Spark with Java

             Copyright : V2 Silvio @2016
                    
Code Samples : Your first Spark Program
*****************************************************************************/
package com.ocidf.spark.bda.train;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class YourFirstSparkProgram {


	@SuppressWarnings("resource")
	public static void main(String[] args) throws IOException {
		
		//Setup configuration
		String appName = "SILVIO";
		String sparkMaster = "local[2]";
		//String sparkMaster = "spark://169.254.86.212:7077";

		JavaSparkContext spContext = null;
	
		SparkConf conf = new SparkConf()
				.setAppName(appName)
				.setMaster(sparkMaster);
		
		//Create Spark Context from configuration
		spContext = new JavaSparkContext(conf);
		
		//Read a file into an RDD
		JavaRDD<String> tweetsRDD = spContext.textFile("data/movietweets.csv");
		
		File workaround = new File(".");
	    System.getProperties().put("hadoop.home.dir", workaround.getAbsolutePath());
	    new File("./bin").mkdirs();
	    new File("./bin/winutils.exe").createNewFile();
		
		//Print first five lines
		for ( String s : tweetsRDD.take(5)) {
			System.out.println(s);
		}
		
		//Print count.
		System.out.println("Total tweets in file : " + tweetsRDD.count());
		
		
		//Convert to upper case
		JavaRDD<String> ucRDD = tweetsRDD.map( str -> str.toUpperCase());
		//Print upper case lines
		for ( String s : ucRDD.take(5)) {
			System.out.println(s);
		}
		
		while(true) {
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}


	}

}

/* Starting your own spark cluster on windows.
 * 
 * Step1 : First go to "SPARK_HOME\bin" Dir and run below commands

spark-class org.apache.spark.deploy.master.Master 
Use the IP and Port provided.

spark-class org.apache.spark.deploy.worker.Worker spark://IP:PORT


*/