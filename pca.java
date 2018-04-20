package bosch.code;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


// $example on$
import java.util.LinkedList;
import java.util.List;

// $example off$

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
// $example on$
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
// $example off$
import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.CollectionsUtils;

//import au.com.bytecode.opencsv.CSVReader;

import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
/**
 * Example for compute principal components on a 'RowMatrix'.
 */

public class pca {

	
//	static double[][] array;
  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("pca1");
    SparkContext sc = new SparkContext(conf);
    RDD<String> textFile = sc.textFile("hdfs://jefferson-city:30102/data2/successTrain.csv", 1);
    
    double[][] success = new double[22000][968];
	   BufferedReader br = new BufferedReader(new FileReader("/s/chopin/a/grad/akmittal/Downloads/successTrain.csv"));

	   for (int i = 0; i < 22000; i++) {
	        String[] success_string = br.readLine().split(",");
		   //String st = br.readLine();
		   //System.out.println(st);
		   //String[] pst = st.split(",");
		   //System.out.println(st.length);
	        for (int j = 0;j < 968; j++) {
	        	if(success_string[j].isEmpty())
	        	{
	        		success_string[j] = "0";
	        	}
	        	else
	        	{
	        		success[i][j] = Double.parseDouble(success_string[j]);
	        	}
	          
	        	
	            //System.out.println(st[i]);
	        	//System.out.println(pst[j]);
	        }
	   }
	   //System.out.println(a[1][0]);
	    double[][] failure = new double[22000][968];
		   BufferedReader br2 = new BufferedReader(new FileReader("/s/chopin/a/grad/akmittal/Downloads/failureTrain.csv"));

		   for (int i = 0; i < 6879; i++) {
		        String[] failure_string = br2.readLine().split(",");
			   //String st = br.readLine();
			   //System.out.println(st);
			   //String[] pst = st.split(",");
			   //System.out.println(st.length);
		        for (int j = 0;j < 968; j++) {
		        	if(failure_string[j].isEmpty())
		        	{
		        		failure_string[j] = "0";
		        	}
		        	else
		        	{
		        		failure[i][j] = Double.parseDouble(failure_string[j]);
		        	}
		          
		        	
		            //System.out.println(st[i]);
		        	//System.out.println(pst[j]);
		        }
		   }
	



    
    LinkedList<Vector> rowsList = new LinkedList<>();
    LinkedList<Vector> rowsList2 = new LinkedList<>();
   // creating the rdd
    for (int i = 0; i < success.length; i++) 
    {
      Vector currentRow = Vectors.dense(success[i]);
      rowsList.add(currentRow);
    }
   for (int i = 0; i < failure.length; i++) 
   {
     Vector currentRow = Vectors.dense(failure[i]);
     rowsList.add(currentRow);
   }
   int full_length = success.length+failure.length;
    JavaRDD<Vector> rows = JavaSparkContext.fromSparkContext(sc).parallelize(rowsList);
    RowMatrix mat = new RowMatrix(rows.rdd());
    Matrix pc = mat.computePrincipalComponents(1);
    for (int i = 0; i < full_length; i++) 
    {
           Vector currentRow2 = Vectors.dense(pc.toArray());
           rowsList2.add(currentRow2);
     }
    JavaRDD<Vector> pca = JavaSparkContext.fromSparkContext(sc).parallelize(rowsList2);
    pca.saveAsTextFile("PrinicpalComponent");
    sc.stop();

  }
}