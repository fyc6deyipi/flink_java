package com.fyc.sink;

import com.fyc.pojo.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.util.List;


public class HbaseSink extends RichSinkFunction<List<UserBehavior>> {

    private static org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
    private static Admin admin;
    private static TableDescriptor tableDescriptor;
    private static Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(configuration);
        admin=connection.getAdmin();
    }

    @Override
    public void invoke(List<UserBehavior> value, Context context) throws Exception {
        TableName tableName = TableName.valueOf("user_behavior");
        // 设置缓存1m，当达到1m时数据会自动刷到hbase，替代了hTable.setWriteBufferSize(30 * 1024 * 1024)
        BufferedMutatorParams params = new BufferedMutatorParams(tableName);
        params.writeBufferSize(1024 * 1024);
        // 创建一个批量异步与hbase通信的对象
        BufferedMutator mutator = connection.getBufferedMutator(params);
        for (UserBehavior u :value) {
            Put put = new Put(Bytes.toBytes(u.hashCode()));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("user_id"), Bytes.toBytes(u.user_id));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("item_id"), Bytes.toBytes(u.item_id));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("category_id"), Bytes.toBytes(u.category_id));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behavior"), Bytes.toBytes(u.behavior));
            put.addColumn(Bytes.toBytes("time"), Bytes.toBytes("ts"), Bytes.toBytes(u.ts));

            mutator.mutate(put);
        }
        // 不用每次put后就调用flush，最后调用就行，这个方法替代了旧api的hTable.setAutoFlush(false, false)
        mutator.flush();
        mutator.close();
        System.out.println("成功了插入了" + value.size() + "行数据");

    }

    @Override
    public void close() throws Exception {
        connection.close(); // 关闭连接
    }

}
