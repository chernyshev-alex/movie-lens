
import com.acme.DFReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.{LocalSparkContext, SparkFunSuite}

class MovieLensSpec extends SparkFunSuite with LocalSparkContext  {

  val spark = SparkSession.builder().master("local")
    .appName("MovieLensSpec").getOrCreate()
  spark.sparkContext.setLogLevel("FATAL")

  val localPath = "./data"

  test("Top 10 best rated movies") {
    val ratings = DFReader.readRatings(localPath)(spark);
    val movies = DFReader.readMovies(localPath)(spark) ;

    // register dataframes as SQL tables
    ratings.createOrReplaceTempView("ratings")
    movies.createOrReplaceTempView("movies")

    // CTE rates10 returns movies that have been rated at least 10 times
    val SQL  = """
      | WITH rates10 AS (
      |   SELECT  MovieID, SUM(Rating) S_RATE FROM ratings
      |   GROUP BY MovieID HAVING COUNT(*) > 10
      | )
      | SELECT movies.MovieID, movies.Title, movies.Genres FROM rates10
      |   INNER JOIN movies ON rates10.MovieID = movies.MovieID
      |   ORDER BY rates10.S_RATE DESC
      |   LIMIT 10
      |""".stripMargin

    spark.sql(SQL).show(10)
  }

  test("Which age group give the most ratings overall") {
    val ratings = DFReader.readRatings(localPath)(spark);
    val users = DFReader.readUsers(localPath)(spark);

    ratings.createOrReplaceTempView("ratings")
    users.createOrReplaceTempView("users")

    // get distribution by ages
    val SQL  = """
            |  SELECT Age, COUNT(*) CNT FROM users U JOIN ratings R ON U.UserID = R.UserID
            |  GROUP BY Age ORDER BY CNT DESC
            |  LIMIT 2 ;
      """.stripMargin

    spark.sql(SQL).show()
  }

  // https://github.com/hhbyyh/DataFrameCheatSheet

  test("With dataset") {
//    val caseClassDS = Seq(Person("Andy", 32)).toDS()
//    val caseClassDS = spark.createDataset(Seq(Person("Andy", 32), Person("Andy2", 33)))
  }

}
