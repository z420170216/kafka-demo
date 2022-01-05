package com.jdwl.wms;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * demo01 简单演示kafka的Producer和Consumer的Api
 *
 * @author zhangwenbo
 * @date 2022/01/05
 */
public class Demo01 {

    String topic = "wms-demo";

    // kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 2 --partitions 2 --topic wms-demo

    @Test
    public void producer() throws ExecutionException, InterruptedException {
        Properties p = new Properties();
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.7:9092,192.168.1.5:9092,192.168.1.6:9092");
        /*
          kafka  持久化数据的MQ  数据-> byte[]，不会对数据进行干预，双方要约定编解码
          kafka是一个app：：使用零拷贝  sendfile 系统调用实现快速数据消费
         */

        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        p.setProperty(ProducerConfig.ACKS_CONFIG, "-1");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(p);
        /*
          现在的producer就是一个提供者，面向的其实是broker，虽然在使用的时候我们期望把数据打入topic

          wms-items
          2partition
          三种商品，每种商品有线性的3个ID
          相同的商品最好去到一个分区里
         */

        while (true) {
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < 3; j++) {
                    ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "item" + j, "val" + i);
                    Future<RecordMetadata> send = producer.send(record);

                    RecordMetadata rm = send.get();
                    int partition = rm.partition();
                    long offset = rm.offset();
                    System.out.println("key: " + record.key() + " val: " + record.value() + " partition: " + partition + " offset: " + offset);
                }
            }
        }
    }

    @Test
    public void consumer() {
        //基础配置
        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.7:9092,192.168.1.5:9092,192.168.1.6:9092");
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 设置consumer-group
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "OOXX");
        // kafka 既是一个 MQ 又是一个 storage
        // 第一次启动，没有offset时，采用的策略
        /*
         *         "What to do when there is no initial offset in Kafka or if the current offset
         *         does not exist any more on the server
         *         (e.g. because that data has been deleted):
         *         <ul>
         *             <li>earliest: automatically reset the offset to the earliest offset
         *             <li>latest: automatically reset the offset to the latest offset</li>
         *             <li>none: throw exception to the consumer if no previous offset is found for the consumer's group</li><li>anything else: throw exception to the consumer.</li>
         *         </ul>";
         */
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        /*
          自动提交时异步提交offset，如果服务宕机，将会出现丢数据、重复消费等情况
          一旦你自动提交，但是是异步的
          1，还没到时间，挂了，没提交，重起一个consuemr，参照offset的时候，会重复消费
          2，一个批次的数据还没写数据库成功，但是这个批次的offset背异步提交了，挂了，重起一个consuemr，参照offset的时候，会丢失消费
         */
        p.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

//        p.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"15000");// 默认5秒 自动提交一次offset
//        p.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,""); // 每次poll消息的最大数量
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);

        //kafka 的consumer会动态负载均衡
        consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("---onPartitionsRevoked:");
                Iterator<TopicPartition> iter = partitions.iterator();
                while (iter.hasNext()) {
                    System.out.println(iter.next().partition());
                }

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("---onPartitionsAssigned:");
                Iterator<TopicPartition> iter = partitions.iterator();

                while (iter.hasNext()) {
                    System.out.println(iter.next().partition());
                }


            }
        });

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));// 0~n

            if (!records.isEmpty()) {
                System.out.println("-----------" + records.count() + "-------------");
                // 每次poll的时候是取多个分区的数据
                // 且每个分区内的数据是有序的
                Set<TopicPartition> partitions = records.partitions();
                /*
                   如果手动提交offset
                   1，按消息进度同步提交
                   2，按分区粒度同步提交
                   3，按当前poll的批次同步提交
                 */
                for (TopicPartition partition : partitions) {
                    List<ConsumerRecord<String, String>> pRecords = records.records(partition);
                    // 在一个微批里，按分区获取poll回来的数据
                    // 线性按分区处理，还可以并行按分区处理用多线程的方式
                    Iterator<ConsumerRecord<String, String>> piter = pRecords.iterator();
                    while (piter.hasNext()) {
                        ConsumerRecord<String, String> next = piter.next();
                        int par = next.partition();
                        long offset = next.offset();
                        String key = next.key();
                        String value = next.value();
                        long timestamp = next.timestamp();

                        System.out.println("key: " + key + " val: " + value + " partition: " + par + " offset: " + offset + "time:: " + timestamp);

                        TopicPartition sp = new TopicPartition(topic, par);
                        OffsetAndMetadata om = new OffsetAndMetadata(offset);
                        HashMap<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                        map.put(sp, om);

                        consumer.commitSync(map);// 这个是最安全的，每条记录级的更新，第一点
                        //单线程，多线程，都可以
                    }
                    /*
                      因为你都分区了
                      拿到了分区的数据集
                      期望的是先对数据整体加工
                      小问题会出现？  你怎么知道最后一条小的offset？！！！！
                      kafka，很傻，你拿走了多少，我不关心，你告诉我你正确的最后一个消息的offset
                     */
                    //获取分区内最后一条消息的offset
                    long poff = pRecords.get(pRecords.size() - 1).offset();

                    OffsetAndMetadata pom = new OffsetAndMetadata(poff);
                    HashMap<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                    map.put(partition, pom);
                    consumer.commitSync(map);//这个是第二种，分区粒度提交offset
                }
                consumer.commitSync();//这个就是按poll的批次提交offset，第3点
            }
        }
    }

}
