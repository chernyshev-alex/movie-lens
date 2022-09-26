package com.acme

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum, _}
import org.apache.spark.sql.types.StructType

case class MovieRating(userId : String, movieId : String, rating : String, Timestamp : BigInt)

object MovieLensApp extends App {
  val RATINGS_FILE_NAME = "ratings.dat"

    val ss = SparkSession.builder().master("local").appName("MovieLensSpec").getOrCreate()
    ss.sparkContext.setLogLevel("WARN")

    val dataConf = Map(RATINGS_FILE_NAME -> List("UserID", "MovieID", "Rating", "Timestamp"))
    val cols = dataConf(RATINGS_FILE_NAME)

    val movieRatingSchema = StructType.fromDDL("UserID int, MovieID int, Rating int, Timestamp bigint")

    implicit  val encoderMovieRating = org.apache.spark.sql.Encoders.product[MovieRating]
    val movieRatingsDs = ss.read.option("delimiter", "::")
          .schema(movieRatingSchema)
          .csv(s"./data/$RATINGS_FILE_NAME").as[MovieRating]

    val ds = movieRatingsDs.groupBy("MovieID").agg(
          sum("Rating").as("TotalRating"),
          count("*").as("Cnt")
    ).where("Cnt > 20")

    val ds2 = ds.joinWith(ds, ds("MovieID") === ds("MovieID"))

    ds2.show(4)

    ss.stop()

}

