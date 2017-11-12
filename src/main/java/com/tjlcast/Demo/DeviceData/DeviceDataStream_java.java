package com.tjlcast.Demo.DeviceData;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.Tuple3;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * Created by tangjialiang on 2017/11/11.
 *
 * mapToPair and map
 * DStream and rdd
 */
public class DeviceDataStream_java {
    public static void main(String[] args) {
        SparkConf deviceData = new SparkConf().setMaster("local[2]").setAppName("DeviceData");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(deviceData, new Duration(1000));

        String host = "10.108.219.61" ;
        int port = 9999 ;
        JavaReceiverInputDStream<String> dStream = javaStreamingContext.socketTextStream(host, port);

        JavaDStream<String> window = dStream.window(new Duration(5000), new Duration(5000));

        // parse input data
        // {"uid":"922291","data":10,"current_time":"2017-11-06 17:44:45"}
        JavaPairDStream<String, Double> uid_data = window.mapToPair(new PairFunction<String, String, Double>() {
            @Override
            public Tuple2<String, Double> call(String s) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                String uid = jsonObject.get("uid").toString();
                double data = Double.parseDouble(jsonObject.get("data").toString());
                return new Tuple2<String, Double>(uid, data);
            }
        });

        // compute avg
        // (uid, data)
        JavaPairDStream<String, Double> uid_total_rdd = uid_data.reduceByKey((a, b) -> (a + b));
        JavaPairDStream<String, Long> uid_count_rdd = uid_data.map(a -> a._1()).countByValue();
        JavaPairDStream<String, Tuple2<Double, Long>> uid_total_count_rdd = uid_total_rdd.join(uid_count_rdd);// ((uid, (total, count)))
        JavaPairDStream<String, Tuple2<Double, Long>> uid_avg_count_rdd = uid_total_count_rdd.mapToPair(s -> new Tuple2<String, Tuple2<Double, Long>>(s._1(), new Tuple2<Double, Long>(s._2()._1() / s._2()._2(), s._2()._2())));//(uid, (avg, count))

        JavaPairDStream<String, Tuple2<Double, Tuple2<Double, Long>>> uid_val_total = uid_data.join(uid_avg_count_rdd);
        // (uid, (data, (avg, count)))
        JavaPairDStream<String, Tuple3<Double, Long, Double>> uid_mid_count = uid_val_total.mapToPair(new PairFunction<Tuple2<String, Tuple2<Double, Tuple2<Double, Long>>>, String, Tuple3<Double, Long, Double>>() {
            @Override
            public Tuple2 call(Tuple2<String, Tuple2<Double, Tuple2<Double, Long>>> stringTuple2Tuple2) throws Exception {
                String uid = stringTuple2Tuple2._1();
                Long count = stringTuple2Tuple2._2()._2()._2();
                Double data = stringTuple2Tuple2._2()._1();
                Double avg = stringTuple2Tuple2._2()._2()._1();
                Double midData = (data - avg) * (data - avg);
                return new Tuple2<String, Tuple3<Double, Long, Double>>(uid, new Tuple3(midData, count, avg));
            }
        }).reduceByKey((a, b) -> new Tuple3<Double, Long, Double>(a._1() + b._1(), a._2(), a._3())); //

        JavaDStream<Tuple2<String, Tuple3<Double, Double, Long>>> result = uid_mid_count.map(new Function<Tuple2<String, Tuple3<Double, Long, Double>>, Tuple2<String, Tuple3<Double, Double, Long>>>() {
            @Override
            public Tuple2<String, Tuple3<Double, Double, Long>> call(Tuple2<String, Tuple3<Double, Long, Double>> e) throws Exception {
                String uid = e._1();
                Double midData = e._2()._1();
                Long count = e._2()._2();
                Double avg = e._2()._3();
                Double variance = midData / count;
                return new Tuple2<String, Tuple3<Double, Double, Long>>(uid, new Tuple3<>(variance, avg, count));
            }
        });

        // 广播KafkaSink
        Broadcast<KafkaSink<String, String>> broadcast = javaStreamingContext.sparkContext().broadcast(new KafkaSink<String, String>());

        result.print() ;
        result.foreachRDD(new VoidFunction<JavaRDD<Tuple2<String, Tuple3<Double, Double, Long>>>>() {
            @Override
            public void call(JavaRDD<Tuple2<String, Tuple3<Double, Double, Long>>> rdd) throws Exception {
                if (!rdd.isEmpty()) {
                    rdd.foreach(new VoidFunction<Tuple2<String, Tuple3<Double, Double, Long>>>() {
                        @Override
                        public void call(Tuple2<String, Tuple3<Double, Double, Long>> record) throws Exception {
                            String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) ;
                            String msg = "(" + record._1()+","+ record._2()._1()+","+record._2()._2()+","+record._2()._3()+","+timestamp+")" ;
                            broadcast.value().send("AnalysisData", msg) ;
                        }
                    });
                }
            }
        });


        javaStreamingContext.start();
        try {
            javaStreamingContext.awaitTermination();
        } catch (InterruptedException e) {
            System.err.println(e);
        }
    }

    // 因为数据域问题，写为内部类
    static class KafkaSink<K, V> {

        private KafkaProducer<K, V> producer = null ;

        private Properties getKafkaConfig() {
            Properties conf = new Properties() ;
            conf.setProperty("bootstrap.servers", "10.108.219.61:9092") ;
            conf.setProperty("key.serializer", StringSerializer.class.getName()) ;
            conf.setProperty("value.serializer", StringSerializer.class.getName()) ;
            return conf ;
        }

        private KafkaProducer<K, V> createProducer() {
            Properties kafkaConfig = getKafkaConfig();
            KafkaProducer<K, V> kvKafkaProducer = new KafkaProducer<>(kafkaConfig);
            return kvKafkaProducer ;
        }

        private KafkaProducer<K, V> getProducer() {
            if (producer == null) {
                producer = createProducer() ;
            }
            return producer ;
        }

        public void send(String topic, K key, V value) {
            getProducer().send(new ProducerRecord<K, V>(topic, key, value)) ;
        }

        public void send(String topic, V value) {
            getProducer().send(new ProducerRecord<K, V>(topic, value)) ;
        }
    }
}



