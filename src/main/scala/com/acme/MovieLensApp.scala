package com.acme

import org.apache.spark.{SparkConf, SparkContext}

object MovieLensApp extends App {

  val config = new SparkConf().setMaster("local[*]").setAppName("movieLens")
  val sparkContext = SparkContext.getOrCreate(config)

  // see test spec
  sparkContext.stop()

}

