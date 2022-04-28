package com.fyc.demo;


import com.alibaba.fastjson.JSON;
import com.fyc.pojo.UserBehavior;
import com.fyc.sink.HbaseSink;
import com.fyc.tools.KAFKA_TOPICS;
import com.fyc.tools.KafkaUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingAlignedProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.List;

public class kafka2redis {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER,true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(3);
        // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(1000);
        // 高级选项：
        // 设置模式为exactly-once （这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoin
        //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                KAFKA_TOPICS.user_behavior,
                new SimpleStringSchema(),
                KafkaUtils.getKafkaPropertise()
        );
        DataStreamSource<String> stream = env.addSource(consumer);
        stream.map((MapFunction<String, UserBehavior>) value -> new UserBehavior().setByJson(value))
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new ProcessAllWindowFunction<UserBehavior, List<UserBehavior>, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<UserBehavior, List<UserBehavior>, TimeWindow>.Context context, Iterable<UserBehavior> iterable, Collector<List<UserBehavior>> collector) throws Exception {
                        collector.collect(IteratorUtils.toList(iterable.iterator()));
                    }
                }).addSink(new HbaseSink()).setParallelism(1);
        env.execute()
        ;

    }
}

