package com.acme

import org.apache.spark.sql.{DataFrame, SparkSession}

object DFReader {

def readDataFrame(ss : SparkSession, filePath : String, cols : String*) : DataFrame = {
  ss.read.option("delimiter", "::").csv(filePath).toDF(cols : _*);
  }

  def readRatings(path : String)(implicit ss : SparkSession) =
  readDataFrame(ss, s"$path/ratings.dat", "UserID", "MovieID", "Rating", "Timestamp")

  def readMovies(path : String)(implicit ss : SparkSession) =
  readDataFrame(ss, s"$path/movies.dat", "MovieID", "Title", "Genres")

  def readUsers(path : String)(implicit ss : SparkSession) =
  readDataFrame(ss, s"$path/users.dat", "UserID" , "Gender", "Age", "Occupation", "Zip-code")

}
