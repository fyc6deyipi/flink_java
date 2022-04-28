package com.fyc.help;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class aliyun_perducer {

    public static final String broker_list = "180.76.51.121:9092";
    public static final String topic = "user_behavior";  //kafka topic 需要和 flink 程序用同一个 topic


    public static void main(String[] args)  throws Exception{
        InputStream inputStream = aliyun_perducer.class.getClassLoader().getResourceAsStream("user_behavior.log");
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        List<String> list = new LinkedList<>();
        String str;
        // 通过readLine()方法按行读取字符串
        while ((str = br.readLine()) != null) {
//            System.out.println(str);
            list.add(str);
        }
        writeToKafka(list);
    }
    public static void writeToKafka(List<String> all) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < all.size(); i++) {
            ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, all.get(i));
            producer.send(record);
            System.out.println(all.get(i));
        }

        producer.flush();
    }
}
