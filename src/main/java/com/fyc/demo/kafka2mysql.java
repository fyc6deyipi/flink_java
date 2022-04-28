package com.fyc.demo;

import com.fyc.pojo.UserBehavior;
import com.fyc.sink.MysqlSink;
import com.fyc.tools.KAFKA_TOPICS;
import com.fyc.tools.KafkaUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingAlignedProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class kafka2mysql {
    private static final Logger log = LoggerFactory.getLogger(kafka2mysql.class);
    public static void main(String[] args) throws Exception {
        log.info("-----------------> start");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
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
        env.addSource(new FlinkKafkaConsumer<String>(
                        KAFKA_TOPICS.user_behavior,
                        new SimpleStringSchema(),
                        KafkaUtils.getKafkaPropertise()))
                .map((MapFunction<String, UserBehavior>) s -> new UserBehavior().setByJson(s))
                .countWindowAll(500)
                .process(new ProcessAllWindowFunction<UserBehavior, List<UserBehavior>, GlobalWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<UserBehavior, List<UserBehavior>, GlobalWindow>.Context context, Iterable<UserBehavior> iterable, Collector<List<UserBehavior>> collector) throws Exception {
                        collector.collect((List<UserBehavior>) IteratorUtils.toList(iterable.iterator()));
                    }
                }).addSink(new MysqlSink()).setParallelism(2);
        env.execute();
    }
}