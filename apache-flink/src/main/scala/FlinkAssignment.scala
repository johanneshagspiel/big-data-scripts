import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import util.Protocol.{Commit, CommitGeo, CommitSummary, File}
import util.{CommitGeoParser, CommitParser, Protocol}

/** Do NOT rename this class, otherwise autograding will fail. **/
object FlinkAssignment {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {

    /**
      * Setups the streaming environment including loading and parsing of the datasets.
      *
      * DO NOT TOUCH!
      */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // Read and parses commit stream.
    val commitStream =
      env
        .readTextFile("data/flink_commits.json")
        .map(new CommitParser)

    // Read and parses commit geo stream.
    val commitGeoStream =
      env
        .readTextFile("data/flink_commits_geo.json")
        .map(new CommitGeoParser)

    /** Use the space below to print and test your questions. */
//    dummy_question(commitStream).print()
    question_seven(commitStream).print()

    /** Start the streaming environment. **/
    env.execute()
  }

  /** Dummy question which maps each commits to its SHA. */
  def dummy_question(input: DataStream[Commit]): DataStream[String] = {
    input.map(_.sha)
  }

  /**
    * Write a Flink application which outputs the sha of commits with at least 20 additions.
    * Output format: sha
    */
  def question_one(input: DataStream[Commit]): DataStream[String] = {

    val pipeline = input
      .filter(x => (x.stats.get.additions >= 20))
      .map(x => x.sha)

    pipeline
  }

  /**
    * Write a Flink application which outputs the names of the files with more than 30 deletions.
    * Output format:  fileName
    */
  def question_two(input: DataStream[Commit]): DataStream[String] = {

    def helper(files: List[File]): List[(String, Int)] = files match {
      case Nil => Nil
      case x :: tail => ((x.filename.get, x.deletions)) :: helper(tail)
    }

    val pipeline = input
      .flatMap(x => helper(x.files))
      .filter(x => (x._2 > 30))
      .map(x => x._1)

    pipeline
  }

  /**
    * Count the occurrences of Java and Scala files. I.e. files ending with either .scala or .java.
    * Output format: (fileName, #occurences)
    */
  def question_three(input: DataStream[Commit]): DataStream[(String, Int)] = {

    def getFileExtension(input: Commit): List[String] = {
      val tempFiles: List[Protocol.File] = input.files
      getFileExtension2(tempFiles)
    }

    def getFileExtension2(input: List[Protocol.File]): List[String] = input match {
      case Nil => Nil
      case x :: tail => getFileExtension3(x.filename.get) :: getFileExtension2(tail)
    }

    def getFileExtension3(input: String): String = {
      val pattern = """[^\.]*$""".r
      val tempFileType: Option[String] = pattern.findFirstIn(input)
      if (tempFileType.isEmpty) "nothing" else tempFileType.get
    }

    val pipeline = input.
      flatMap(x => getFileExtension(x)).
      filter(x => (x.equals("scala") || x.equals("java"))).
      map(x => (x, 1)).
      keyBy(0).
      sum(1)

    pipeline
  }

  /**
    * Count the total amount of changes for each file status (e.g. modified, removed or added) for the following extensions: .js and .py.
    * Output format: (extension, status, count)
    */
  def question_four(input: DataStream[Commit]): DataStream[(String, String, Int)] = {

    val pipeline = input.
      flatMap(x => x.files).
      filter(x => (!x.status.isEmpty && !x.filename.isEmpty)).
      map(x => (x.filename.get, x.status.get, x.changes)).
      filter(x => (x._1.endsWith(".js") || x._1.endsWith(".py"))).
      map(x => (if (x._1.endsWith(".js")) ("js", x._2, x._3) else ("py", x._2, x._3))).
      keyBy(0,1).
      sum(2)

    pipeline
  }

  /**
    * For every day output the amount of commits. Include the timestamp in the following format dd-MM-yyyy; e.g. (26-06-2019, 4) meaning on the 26th of June 2019 there were 4 commits.
    * Make use of a non-keyed window.
    * Output format: (date, count)
    */
  def question_five(input: DataStream[Commit]): DataStream[(String, Int)] = {

    def helperDate(dateIn: Date): String = {
      val tempTime = new SimpleDateFormat("dd-MM-yyyy")
      val result = tempTime.format(dateIn)
      result
    }

    val pipeline = input.
      assignAscendingTimestamps(x => x.commit.committer.date.getTime).
      map(x => (helperDate(x.commit.committer.date), 1)).
      windowAll(TumblingEventTimeWindows.of(Time.days(1))).
      reduce((x,y) => (x._1, x._2 + y._2))

    pipeline
  }

  /**
    * Consider two types of commits; small commits and large commits whereas small: 0 <= x <= 20 and large: x > 20 where x = total amount of changes.
    * Compute every 12 hours the amount of small and large commits in the last 48 hours.
    * Output format: (type, count)
    */
  def question_six(input: DataStream[Commit]): DataStream[(String, Int)] = {

    def helperClassification(changesTotal: Int): String = {
      if (0 <= changesTotal && changesTotal <= 20) "small" else "large"
    }

    val pipeline = input
      .assignAscendingTimestamps(x => x.commit.committer.date.getTime)
      .map(x => (helperClassification(x.stats.get.total), 1))
      .keyBy(0)
      .window(SlidingEventTimeWindows.of(Time.hours(48), Time.hours(12)))
      .reduce((x,y) => (x._1, x._2 + y._2))

    pipeline
  }

  /**
    * For each repository compute a daily commit summary and output the summaries with more than 20 commits and at most 2 unique committers. The CommitSummary case class is already defined.
    *
    * The fields of this case class:
    *
    * repo: name of the repo.
    * date: use the start of the window in format "dd-MM-yyyy".
    * amountOfCommits: the number of commits on that day for that repository.
    * amountOfCommitters: the amount of unique committers contributing to the repository.
    * totalChanges: the sum of total changes in all commits.
    * topCommitter: the top committer of that day i.e. with the most commits. Note: if there are multiple top committers; create a comma separated string sorted alphabetically e.g. `georgios,jeroen,wouter`
    *
    * Hint: Write your own ProcessWindowFunction.
    * Output format: CommitSummary
    */
  def question_seven(commitStream: DataStream[Commit]): DataStream[CommitSummary] = {

    class MyProcessWindowFunction extends ProcessWindowFunction[(String, String, Int), CommitSummary, String, TimeWindow] {

      def helperMostPopular(input: Iterable[String]): String = {

        val test = input.toList.groupBy(identity).mapValues(_.size)
        val max = test.maxBy(_._2)
        val test3 = test.filter(x => (x._2.equals(max._2))).toList
        val result = helperConcat(test3)

        result.dropRight(1)
      }

      def helperConcat(input: List[(String, Int)]): String = input match {
        case Nil => ""
        case x :: tail => x._1 + "," + helperConcat(tail)
      }

      def helperDate(dateIn: Long): String = {
        val tempTime = new SimpleDateFormat("dd-MM-yyyy")
        val result = tempTime.format(dateIn)
        result
      }

      def helperUniqueContributors(input: Iterable[String]): Int = {
        input.toList.distinct.size
      }

      override def process(key: String, context: Context, elements: Iterable[(String, String, Int)], collector: Collector[CommitSummary]): Unit = {

        val repoName = key
        val date = helperDate(context.window.getStart)
        val amountOfCommits = elements.size
        val totalChangesIn = elements.map(x => x._3).sum
        val amountOfCommitters = helperUniqueContributors(elements.map((x => x._2)))
        val mostPopularCommits = helperMostPopular(elements.map((x => x._2)))

        val result = CommitSummary(repoName, date, amountOfCommits, amountOfCommitters, totalChangesIn, mostPopularCommits)
        collector.collect(result)
      }
    }

    def helperGetRepositoryName(urlIn: String): String = {
      val pattern = """(/repos)(/.*)""".r
      val repositoryNameTemp: Option[String] = pattern.findFirstIn(urlIn)
      val repositoryName = repositoryNameTemp.getOrElse("SomethingWentWrong").substring(7)
      val repositorySplit = repositoryName.split("/")
      val repositoryResult = repositorySplit(0) + "/" + repositorySplit(1);
      repositoryResult
    }

    val pipeline = commitStream
      .assignAscendingTimestamps(x => x.commit.committer.date.getTime)
      .map(x =>(helperGetRepositoryName(x.url), x.commit.committer.name, x.stats.get.total))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.days(1)))
      .process(new MyProcessWindowFunction())
      .filter(x => (x.amountOfCommits > 20 && x.mostPopularCommitter.split(",").size<3))

    pipeline
  }

  /**
    * For this exercise there is another dataset containing CommitGeo events. A CommitGeo event stores the sha of a commit, a date and the continent it was produced in.
    * You can assume that for every commit there is a CommitGeo event arriving within a timeframe of 1 hour before and 30 minutes after the commit.
    * Get the weekly amount of changes for the java files (.java extension) per continent.
    *
    * Hint: Find the correct join to use!
    * Output format: (continent, amount)
    */

  def question_eight(commitStream: DataStream[Commit],geoStream: DataStream[CommitGeo]): DataStream[(String, Int)] = {

    class MyProcessWindowFunction extends ProcessJoinFunction[Commit, Protocol.CommitGeo, (String, Int)] {

      override def processElement(in1: Commit, in2: CommitGeo, context: ProcessJoinFunction[Commit, CommitGeo, (String, Int)]#Context, collector: Collector[(String, Int)]): Unit =
      {
        in1.files.filter(x => (x.filename.getOrElse("sdfsdf").endsWith(".java")))
          .foreach(x => collector.collect((in2.continent, x.changes)))
      }
    }

    def helper(commit: Commit): List[(String, String, Int)] = {
      val result = helperGetFileLevel(commit.sha, commit.files)
      result
    }

    def helperGetFileLevel(sha: String, files: List[File]): List[(String, String, Int)] = files match {
      case Nil => Nil
      case x::tail => (sha, getFileExtension(x.filename.get), x.changes) :: helperGetFileLevel(sha, tail)
    }

    def getFileExtension(input: String): String = {
      val pattern = """[^\.]*$""".r
      val tempFileType: Option[String] = pattern.findFirstIn(input)
      if (tempFileType.isEmpty) "nothing" else tempFileType.get
    }

    val commitStreamTemp = commitStream
      .assignAscendingTimestamps(x => x.commit.committer.date.getTime)
      .keyBy(_.sha)

    val temp = geoStream
      .assignAscendingTimestamps(x => x.createdAt.getTime)
      .keyBy(_.sha)

    val pipeline = commitStreamTemp
      .intervalJoin(temp)
      .between(Time.hours(-1), Time.minutes(30))
      .process(new MyProcessWindowFunction())
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.days(7)))
      .reduce((x,y) => (x._1, x._2 + y._2))

    pipeline
  }

  /**
    * Find all files that were added and removed within one day. Output as (repository, filename).
    *
    * Hint: Use the Complex Event Processing library (CEP).
    * Output format: (repository, filename)
    */
  def question_nine(inputStream: DataStream[Commit]): DataStream[(String, String)] = {

    def helper(commit: Commit): List[(String, String, String)] = {
      val result = helperGetFileLevel(helperGetRepositoryName(commit.url), commit.files)
      result
    }

    def helperGetFileLevel(repository: String, files: List[File]): List[(String, String, String)] = files match {
      case Nil => Nil
      case x::tail => (repository, x.filename.get, x.status.get) :: helperGetFileLevel(repository, tail)
    }

    def helperGetRepositoryName(urlIn: String): String = {
      val pattern = """(/repos)(/.*)""".r
      val repositoryNameTemp: Option[String] = pattern.findFirstIn(urlIn)
      val repositoryName = repositoryNameTemp.getOrElse("SomethingWentWrong").substring(7)
      val repositorySplit = repositoryName.split("/")
      val repositoryResult = repositorySplit(0) + "/" + repositorySplit(1)
      repositoryResult
    }

    val pipeline = inputStream
      .assignAscendingTimestamps(x => x.commit.committer.date.getTime)
      .flatMap(x => helper(x))
      .filter(x => (x._3.equals("added") || x._3.equals("removed")))
      .map(x => (x._1, x._2))
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.days(1)))
      .reduce((x,y) => (x._1, x._2 + ", " + y._2))

    pipeline
  }

}
