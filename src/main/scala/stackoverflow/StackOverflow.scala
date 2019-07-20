package stackoverflow

import java.time.temporal.ChronoUnit
import java.time.{LocalDateTime, ZoneId}

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.log4j.lf5.LogLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.collection._
import scala.util.Random
import org.scalameter._

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int,
                   id: Int,
                   acceptedAnswer: Option[Int],
                   parentId: Option[QID],
                   score: Int,
                   tags: Option[String])
    extends Serializable

/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw: RDD[Answer] = rawPostings(lines)
    val grouped: RDD[(QID, Iterable[(Question, Answer)])] = groupedPostings(raw)
    val scored: RDD[(Question, HighScore)] = scoredPostings(grouped)

    val start = LocalDateTime.now
    val vectors: RDD[(LangIndex, HighScore)] = vectorPostings(scored)
    val means = kmeans(sampleVectors(vectors), vectors, debug = false)
    val results = clusterResults(means, vectors)
    val end = LocalDateTime.now
    val startWithZone = start.atZone(ZoneId.systemDefault())
    val endWithZone = end.atZone(ZoneId.systemDefault())

    val diff = startWithZone.until(endWithZone, ChronoUnit.SECONDS)
    println(s"SECONDS: $diff s")
    printResults(results)

    val startP = LocalDateTime.now
    val partitionedVectorsP: RDD[(LangIndex, HighScore)] = vectorPostings(scored, partition = true)
    val partitionedMeansP = kmeans(sampleVectors(partitionedVectorsP), partitionedVectorsP, debug = false)
    val partitionedResultsP = clusterResults(partitionedMeansP, partitionedVectorsP)
    val endP = LocalDateTime.now
    val startPWithZone = startP.atZone(ZoneId.systemDefault())
    val endPWithZone = endP.atZone(ZoneId.systemDefault())

    val partitionedDiff = startPWithZone.until(endPWithZone, ChronoUnit.SECONDS)
    println(s"SECONDS P: $partitionedDiff s")
    printResults(partitionedResultsP)

  }
}

/** The parsing and kmeans methods */
class StackOverflow extends Serializable {

  /** Languages */
  val langs =
    List("JavaScript",
         "Java",
         "PHP",
         "Python",
         "C#",
         "C++",
         "Ruby",
         "CSS",
         "Objective-C",
         "Perl",
         "Scala",
         "Haskell",
         "MATLAB",
         "Clojure",
         "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120

  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(
        postingType = arr(0).toInt,
        id = arr(1).toInt,
        acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
        parentId = if (arr(3) == "") None else Some(arr(3).toInt),
        score = arr(4).toInt,
        tags = if (arr.length >= 6) Some(arr(5).intern()) else None
      )
    })

  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {
    val questions: RDD[(QID, Question)] = postings
      .filter(posting => posting.postingType == 1)
      .map(posting => (posting.id, posting))

    val answers: RDD[(QID, Answer)] = postings
      .filter(posting => posting.postingType == 2 && posting.parentId.nonEmpty)
      .map(posting => (posting.parentId.get, posting))

    questions.join(answers).groupByKey()
  }

  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {

    def answerHighScore(as: Array[Answer]): HighScore = {
      var highScore = 0
      var i = 0
      while (i < as.length) {
        val score = as(i).score
        if (score > highScore)
          highScore = score
        i += 1
      }
      highScore
    }

    grouped.map {
      case (qid, pairs) =>
        val question = pairs.head._1
        val answers = pairs.map { case (q, a) => a }.toArray
        val highScore = answerHighScore(answers)
        (question, highScore)
    }
  }

  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Question, HighScore)], partition: Boolean = false): RDD[(LangIndex, HighScore)] = {

    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None    => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }

    val vectors = scored
      .map {
        case (q, hs) => (firstLangInTag(q.tags, langs).getOrElse(-1) * langSpread, hs)
      }

    if (partition) {
      vectors
        .partitionBy(new HashPartitioner(langs.length)) // One Partition per language
        .cache()
    } else {
      vectors.cache()
    }
  }

  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
        // sample the space regardless of the language
        vectors.takeSample(withReplacement = false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey
          .flatMap({
            case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
          })
          .collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }

  //
  //
  //  Kmeans method:
  //
  //

  /**
    * Main kmeans computation
    *
    * Based on the initial means and the provided variables converged method,
    * implement the K-means algorithm by iteratively:
    *   - pairing each vector with the index of the closest mean (its cluster);
    *   - computing the new means by averaging the values of each cluster.
    *
    * To implement these iterative steps, use the provided functions findClosest, averageVectors and euclideanDistance.
    *
    * @param means
    * @param vectors
    * @param iter
    * @param debug
    * @return
    */
  @tailrec final def kmeans(means: Array[(LangIndex, HighScore)],
                            vectors: RDD[(LangIndex, HighScore)],
                            iter: Int = 1,
                            debug: Boolean = false): Array[(LangIndex, HighScore)] = {

    // Pair each vector with the index of the closest mean (its cluster)
    val closestIndexForEachVector: RDD[(Int, (LangIndex, HighScore))] = vectors
      .map(point => (findClosest(point, means), point))

    //computing the new means by averaging the values of each cluster.
    val empty = List.empty[(LangIndex, HighScore)]
    val newComputedMeansPairs: Array[(Int, (LangIndex, HighScore))] =
      closestIndexForEachVector
        .groupByKey()
        .mapValues(averageVectors)
        .collect()

    val newMeans: Array[(LangIndex, LangIndex)] = means.clone()

    for ((langIndex, meanPair) <- newComputedMeansPairs) {
      newMeans.update(langIndex, meanPair)
    }

    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
        println(
          f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
            f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      if (debug) {
        println("Reached max iterations!")
      }
      newMeans
    }
  }

  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double): Boolean =
    distance < kmeansEta

  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while (idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (LangIndex, HighScore), centers: Array[(LangIndex, HighScore)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- centers.indices) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }

  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }

  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(LangIndex, HighScore)],
                     vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {
    val closest: RDD[(LangIndex, (LangIndex, HighScore))] = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped: RDD[(LangIndex, Iterable[(LangIndex, HighScore)])] = closest.groupByKey()

    val median = closestGrouped.mapValues { vs: Iterable[(LangIndex, HighScore)] =>
      val pairsGroupedByLang: Map[LangIndex, Iterable[(LangIndex, HighScore)]] = vs.groupBy(p => p._1)
      val sizesGroupedByLang: Map[LangIndex, Int] = pairsGroupedByLang.mapValues[Int](vals => vals.size)
      val langWithHighestScore: (LangIndex, Int) = sizesGroupedByLang
        .maxBy(p => p._2) // Get pair with highest score

      // most common language in the cluster
      val langLabel: String = langs(langWithHighestScore._1 / langSpread)

      // percent of the questions in the most common language
      val langPercent: Double = sizesGroupedByLang(langWithHighestScore._1) * 100 / vs.size

      val clusterSize: Int = vs.size

      val medianScore: Int = {
        val listOfScores: List[HighScore] = pairsGroupedByLang(langWithHighestScore._1)
          .map(p => p._2)
          .toList
        medianCalculator(listOfScores)
      }

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }

  def medianCalculator(seq: Seq[Int]): Int = {
    val sortedSeq = seq.sortWith(_ < _)

    if (seq.size % 2 == 0) {
      val (left, right) = sortedSeq.splitAt(seq.size / 2)
      (left.last + right.head) / 2
    } else {
      sortedSeq(sortedSeq.size / 2)
    }
  }

}
