package com.tjlcast.Demo.DeviceData

/**
  * Created by tangjialiang on 2017/11/12.
  */



import java.text.SimpleDateFormat
import java.util.concurrent.Future
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json.JSON

object DeviceData {
    def main(args: Array[String]): Unit = {
        System.out.println("hello world")

        val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
        val ssc = new StreamingContext(conf, Seconds(2))

        // receive data from kafka
        val source = new KafkaDataSource().getDstream(ssc)
        val lines = source.map(s => s.value())

        // receive data from socket
        // val lines = new SocketDataSource().getDstream(ssc)

        val windows = lines.window(Seconds(10), Seconds(10))
        //// {"uid":"922291","data":10,"current_time":"2017-11-06 17:44:45"}

        def regJson(json:Option[Any]) = json match {
            case Some(map: Map[String, Any]) => map
        }
        def parseDouble(s: String): Option[Double] = try { Some(s.toDouble) } catch { case _ => Some(0.0) }

        val itemRdd = windows.map(str => {
            val jsonS = JSON.parseFull(str)
            val first = regJson(jsonS)
            (first.get("uid").get.toString, parseDouble(first.get("data").get.toString).get)
        })// (uid, data)

        // compute avg
        val uid_total_rdd = itemRdd.reduceByKey(_ + _)
        val uid_count_rdd = itemRdd.map(a => a._1).countByValue()
        val uid_total_count_rdd = uid_total_rdd.join(uid_count_rdd) // (uid, (total, count))
        val uid_avg_count_rdd = uid_total_count_rdd.map(s => (s._1, (s._2._1/s._2._2, s._2._2))) // (uid, avg, count)

        val uid_val_total = itemRdd.join(uid_avg_count_rdd) // (uid, (data, (avg, count)))

        val uid_mid_count = uid_val_total.map( e => {
            val uid = e._1
            val count = e._2._2._2
            val data = e._2._1
            val avg = e._2._2._1
            val midData = (data - avg) * (data - avg)
            (uid, (midData, count, avg))
        }).reduceByKey((a, b)=> (a._1+b._1, a._2, a._3))// (uid, (midData(total), count， avg))

        val result = uid_mid_count.map( e => {
            val uid = e._1
            val midData = e._2._1
            val count = e._2._2
            val avg = e._2._3
            val variance = midData / count
            (uid, (variance, avg, count))
        })// (uid, (var, avg, count)) uid unique

        // 广播KafkaSink
        val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
            val kafkaProducerConfig = {
                val p = new Properties()
                p.setProperty("bootstrap.servers", "10.108.219.61:9092")
                p.setProperty("key.serializer", classOf[StringSerializer].getName)
                p.setProperty("value.serializer", classOf[StringSerializer].getName)
                p
            }
            ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
        }

        // todo send to kafka
        result.print()
        result.foreachRDD(rdd => {
            if (!rdd.isEmpty) {
                rdd.foreach(record => {
                    val timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime)
                    val msg = (record._1, record._2._1, record._2._2, record._2._3, timestamp)
                    kafkaProducer.value.send("AnalysisData", msg.toString())
                })
            }
        })

        ssc.start()
        ssc.awaitTermination()
    }
}



class SocketDataSource extends Serializable{
    // val lines = ssc.socketTextStream("10.108.219.61", 9999)
    def getDstream(ssc: StreamingContext): org.apache.spark.streaming.dstream.ReceiverInputDStream[scala.Predef.String] = {
        val stream = ssc.socketTextStream("10.108.219.61", 9999)
        return stream
    }
}


class KafkaDataSource extends Serializable {
    val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "10.108.219.61:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "use_a_separate_group_id_for_each_stream",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    def getDstream(ssc: StreamingContext): org.apache.spark.streaming.dstream.InputDStream[org.apache.kafka.clients.consumer.ConsumerRecord[String, String]] = {
        val topics = Array("device")
        val stream = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )
        return stream
    }
}




class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {
    /* This is the key idea that allows us to work around running into
       NotSerializableExceptions. */
    lazy val producer = createProducer() // 惰性变量只能是不可变变量，并且只有在调用惰性变量时，才会去实例化这个变量。
    def send(topic: String, key: K, value: V): Future[RecordMetadata] =
        producer.send(new ProducerRecord[K, V](topic, key, value))
    def send(topic: String, value: V): Future[RecordMetadata] =
        producer.send(new ProducerRecord[K, V](topic, value))
}

object KafkaSink {
    import scala.collection.JavaConversions._
    def apply[K, V](config: Map[String, Object]): KafkaSink[K, V] = {
        val createProducerFunc = () => {
            val producer = new KafkaProducer[K, V](config)
            sys.addShutdownHook {
                // Ensure that, on executor JVM shutdown, the Kafka producer sends
                // any buffered messages to Kafka before shutting down.
                producer.close()
            }
            producer
        }
        new KafkaSink(createProducerFunc)
    }
    def apply[K, V](config: java.util.Properties): KafkaSink[K, V] = apply(config.toMap)
}