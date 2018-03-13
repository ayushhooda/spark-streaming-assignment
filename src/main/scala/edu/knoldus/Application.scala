package edu.knoldus

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.log4j.Logger
import java.sql.DriverManager

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Application extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Twitter-Assignment")
  val sc = new SparkContext(conf)

  val config = ConfigFactory.load("application.conf")
  val consumerKey = config.getString("consumerKey")
  val consumerSecret = config.getString("consumerSecret")
  val accessToken = config.getString("accessToken")
  val accessTokenSecret = config.getString("accessTokenSecret")
  val username = config.getString("dbUserName")
  val password = config.getString("dbPassword")
  val logger = Logger.getLogger(this.getClass)

  val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().setDebugEnabled(true)
    .setDebugEnabled(false)
    .setOAuthConsumerKey(consumerKey)
    .setOAuthConsumerSecret(consumerSecret)
    .setOAuthAccessToken(accessToken)
    .setOAuthAccessTokenSecret(accessTokenSecret)
    .build()))

  val JDBC_DRIVER = "com.mysql.jdbc.Driver"
  Class.forName("com.mysql.jdbc.Driver")
  val DB_URL = "jdbc:mysql://localhost/twitterDb"
  val conn = DriverManager.getConnection(DB_URL, username, password)
  val stmt = conn.createStatement

  val ssc = new StreamingContext(sc, Seconds(5))
  val stream = TwitterUtils.createStream(ssc, auth)

  val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

  val topCounts: DStream[(Int, String)] = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
    .map { case (topic, count) => (count, topic) }
    .transform(_.sortByKey(false))

  topCounts.foreachRDD(rdd => {
    val topThreeList = rdd.take(3)
    logger.info("\nTop three hashTags in last 10 seconds (%s total):\n".format(rdd.count()))
    topThreeList.foreach { case (count, tag) =>
      logger.info("%s (%s tweets)\n".format(tag, count))
      val query = " insert into twitter_table (hashTag, count)" + " values (?, ?)"
      val preparedStmt = conn.prepareStatement(query)
      preparedStmt.setString(1, tag)
      preparedStmt.setInt(2, count)
      preparedStmt.execute
    }
  })

  ssc.start()
  ssc.awaitTermination()
  conn.close()
}
