package com.frequent.item;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
// $example off$
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;

public class JavaSimpleFPGrowth {
  private static final double THRESHOLD = 0.5; // threshold = 50%
  private static final int COUNT = 2;
  public static void main(String[] args) {
	// initializing spark
    SparkSession spark = SparkSession.builder().config("spark.master","local[*]").getOrCreate();
	JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
	sc.setLogLevel("WARN");
	
	// create RDD by using text files
    JavaRDD<String> data = sc.textFile("F:\\work\\Project5(Algorithm)\\FrequentItem\\sample_fpgrowth.txt");

    JavaRDD<List<String>> baskets = data.map(line -> Arrays.asList(line.split(" ")));

    //total number of baskets
 	Broadcast<Long> basketCount = sc.broadcast(baskets.count());
 	
    FPGrowth fpg = new FPGrowth()
      .setMinSupport(0.2)
      .setNumPartitions(10);
    FPGrowthModel<String> model = fpg.run(baskets);

    int s = (int) Math.ceil(THRESHOLD * basketCount.getValue());
    
    System.out.println("final double frequent pairs => ");
    for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
      // show only double frequent pairs.
      if(itemset.javaItems().size() == COUNT && itemset.freq() >= s)
    	  System.out.println("(" + itemset.javaItems() + "), " + itemset.freq());
    }

    /*
    double minConfidence = 0.8;
    for (AssociationRules.Rule<String> rule
      : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
      System.out.println(
        rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
    }
    // $example off$
	*/
    
    sc.stop();
    sc.close();
  }
}
