//https://vanwilgenburg.wordpress.com/2016/09/19/getting-started-with-akka-stream-kafka/
package net.jvw

import java.time.Instant

import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, KafkaConsumerActor, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.{Await, Future}
import scala.util.Success
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object MainHongwei extends App {

  val config = ConfigFactory.load()
  implicit val system = ActorSystem.create("test", config)
  implicit val mat = ActorMaterializer()
  
  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers("localhost:9092")
    .withGroupId("hongwei-test-5")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  
  
//  def createKafkaConsumer= {
//    val kafkaConsumer= system.actorOf(KafkaConsumerActor.props(consumerSettings))
//    kafkaConsumer
//  }
//  
//  
//   //https://doc.akka.io/docs/akka-stream-kafka/current/consumer.html#sharing-kafkaconsumer
//  private val kafkaConsumer: ActorRef = createKafkaConsumer
//  
//  //we set up the AkkaStreams with one sharing-kafka-consumer pattern
//  //Important: this is the sharing pattern 
//  def consumerStream (topic: String, partition: Int): Source[ConsumerRecord[String, String], Consumer.Control] = {
//    val consumerStream =Consumer
//    .plainExternalSource[String, String](kafkaConsumer, Subscriptions.assignment(new TopicPartition(topic, partition)))
//    consumerStream
//  }
//  
//  
//
//  val returnValue: Future[String] = consumerStream("obp.JuneYellow2017.S.OutboundGetBank",0)
//    .map(msg => {
//      println("xxxxxxx"+msg)
//      msg.value()
//    })
//    .runWith(Sink.head)
//  
//  returnValue.onComplete{
//                          case Success(f) =>  println("yyyyy"+f)
//                        }
//  
//  
//  
//  val returnValue2: Future[String] = consumerStream("obp.JuneYellow2017.S.OutboundGetAdapterInfo",0)// Consumer.plainSource(consumerSettings, Subscriptions.topics("obp.JuneYellow2017.S.OutboundGetAdapterInfo"))
//    .map(msg => {
//      println("xxxxxxx"+msg)
//      msg.value()
//    })
//    .runWith(Sink.head)
//
//  returnValue2.onComplete{
//                          case Success(f) =>  println("yyyyy"+f)
//                        }
  
  
  val done =
  Consumer.committableSource(consumerSettings, Subscriptions.topics("obp.JuneYellow2017.S.OutboundGetBank"))
    .mapAsync(3) { msg =>
      println("xxxyyyyy"+msg.record.value())
      Future{msg}
    }
    .mapAsync(3) { msg =>
      msg.committableOffset.commitScaladsl()
    }
    .runWith(Sink.ignore)
  
  
  

  // prevent WakeupException on quick restarts of vm
  scala.sys.addShutdownHook {
    println("Terminating... - " + Instant.now)
    system.terminate()
    Await.result(system.whenTerminated, 30 seconds)
    println("Terminated... Bye - " + Instant.now)
  }

  
  
}