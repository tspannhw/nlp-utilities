import java.util.Properties
 
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.phoenix.spark._
 
import com.vader.SentimentAnalyzer
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
 
case class Tweet(coordinates: String, geo:String, handle: String, hashtags: String, language: String,
                 location: String, msg: String, time: String, tweet_id: String, unixtime: String, user_name: String, tag: String, profile_image_url: String,
                 source: String, place: String, friends_count: String, followers_count: String, retweet_count: String, 
                 time_zone: String, sentiment: String, stanfordSentiment: String)
 
val message = convert(anyMessage)
  val pipeline = new StanfordCoreNLP(nlpProps)
  val annotation = pipeline.process(message)
  var sentiments: ListBuffer[Double] = ListBuffer()
  var sizes: ListBuffer[Int] = ListBuffer()
 
  var longest = 0
  var mainSentiment = 0
 
  for (sentence <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
    val tree = sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree])
    val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
    val partText = sentence.toString
 
    if (partText.length() > longest) {
      mainSentiment = sentiment
      longest = partText.length()
    }
 
    sentiments += sentiment.toDouble
    sizes += partText.length
  }
 
  val averageSentiment:Double = {
    if(sentiments.nonEmpty) sentiments.sum / sentiments.size
    else -1
  }
 
  val weightedSentiments = (sentiments, sizes).zipped.map((sentiment, size) => sentiment * size)
  var weightedSentiment = weightedSentiments.sum / (sizes.fold(0)(_ + _))
 
  if(sentiments.isEmpty) {
    mainSentiment = -1
    weightedSentiment = -1
  }
 
  weightedSentiment match {
    case s if s <= 0.0 => NOT_UNDERSTOOD
    case s if s < 1.0 => VERY_NEGATIVE
    case s if s < 2.0 => NEGATIVE
    case s if s < 3.0 => NEUTRAL
    case s if s < 4.0 => POSITIVE
    case s if s < 5.0 => VERY_POSITIVE
    case s if s > 5.0 => NOT_UNDERSTOOD
  }
}
 
trait SENTIMENT_TYPE
case object VERY_NEGATIVE extends SENTIMENT_TYPE
case object NEGATIVE extends SENTIMENT_TYPE
case object NEUTRAL extends SENTIMENT_TYPE
case object POSITIVE extends SENTIMENT_TYPE
case object VERY_POSITIVE extends SENTIMENT_TYPE
case object NOT_UNDERSTOOD extends SENTIMENT_TYPE
