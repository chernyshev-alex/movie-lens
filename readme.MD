## Tasks

1. Which are the Top 10 best rated movies (consider only movies which have at least 10 ratings)?
2. Which age group give the most ratings overall?

@see src/test/scala MovieLensSpec

1. test : "Top 10 best rated movies"
   Movies that have been rated at least 10 times, get summary rating, order by rating
   and take first 10
   
2. test "Which age group give the most ratings overall"

Prerequisites :  see build.sbt

How to test :  $>  sbt test 

Test will print out the result for Tasks 1, 2

Expected result : 

+-------+--------------------+--------------------+
|MovieID|               Title|              Genres|
+-------+--------------------+--------------------+
|   2858|American Beauty (...|        Comedy|Drama|
|    260|Star Wars: Episod...|Action|Adventure|...|
|   1196|Star Wars: Episod...|Action|Adventure|...|
|   1210|Star Wars: Episod...|Action|Adventure|...|
|   2028|Saving Private Ry...|    Action|Drama|War|
|   1198|Raiders of the Lo...|    Action|Adventure|
|    593|Silence of the La...|      Drama|Thriller|
|   2571|  Matrix, The (1999)|Action|Sci-Fi|Thr...|
|   2762|Sixth Sense, The ...|            Thriller|
|    589|Terminator 2: Jud...|Action|Sci-Fi|Thr...|
+-------+--------------------+--------------------+

+---+------+
|Age|   CNT|
+---+------+
| 25|395556|
| 35|199003|
+---+------+

[info] MovieLensSpec:
[info] - Top 10 best rated movies
[info] - Which age group give the most ratings overall
[info] Run completed in 13 seconds.
[info] Total number of tests run: 2
[info] Suites: completed 1, aborted 0
[info] Tests: succeeded 2, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
[success] Total time: 21 s, completed Sep 15, 2022, 3:14:34 PM





