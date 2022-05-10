package com.fyc.demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fyc.tools.KAFKA_TOPICS;
import com.fyc.tools.KafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingAlignedProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.awt.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Calendar;
import java.util.Date;

public class kafka2redis {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(3);
        //水位线周期生成时间，默认200ms
        env.getConfig().setAutoWatermarkInterval(200L);
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
        stream.map((MapFunction<String, Tuple4<String, String, Long, Integer>>) value -> {
                    JSONObject jsonObject = JSON.parseObject(value);
                    Tuple2<String, Long> tuple2 = UTC2BJ(jsonObject.getString("ts"));
                    return Tuple4.of(jsonObject.getString("behavior"), tuple2.f0.substring(0, 18), tuple2.f1, 1);
                }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.LONG, Types.INT))
                .filter((FilterFunction<Tuple4<String, String, Long, Integer>>) value -> value.f0.equals("buy"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, Long, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, Long, Integer>>() {
                            @Override
                            public long extractTimestamp(Tuple4<String, String, Long, Integer> element, long recordTimestamp) {
                                return element.f2;
                            }
                        })).keyBy(t -> t.f1)

                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple4<String, String, Long, Integer>>() {
                    @Override
                    public Tuple4<String, String, Long, Integer> reduce(Tuple4<String, String, Long, Integer> value1, Tuple4<String, String, Long, Integer> value2) throws Exception {
                        return Tuple4.of(value1.f0, value1.f1, value1.f2, value2.f3 + value1.f3);
                    }
                }).process(new ProcessFunction<Tuple4<String, String, Long, Integer>, Tuple2<String,Integer>>() {
                    @Override
                    public void processElement(Tuple4<String, String, Long, Integer> stringStringLongIntegerTuple4, ProcessFunction<Tuple4<String, String, Long, Integer>, Tuple2<String, Integer>>.Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        collector.collect(Tuple2.of(stringStringLongIntegerTuple4.f1,stringStringLongIntegerTuple4.f3));
                    }
                }).print();


        env.execute();

    }

    private static Tuple2<String, Long> UTC2BJ(String value) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(value));
        calendar.set(Calendar.HOUR, calendar.get(Calendar.HOUR) + 8);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = calendar.getTime();
        String date2 = simpleDateFormat.format(calendar.getTime());
        return Tuple2.of(date2, date.getTime());
    }
}
