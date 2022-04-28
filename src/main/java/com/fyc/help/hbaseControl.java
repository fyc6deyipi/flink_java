package com.fyc.help;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class hbaseControl {
    private static Configuration cfg = HBaseConfiguration.create();
    private static Admin admin;
    private static TableDescriptor tableDescriptor;
    private static Connection connection;

    static {
        try {
            cfg.set("hbase.zookeeper.quorum", "hadoop:2181");
            connection = ConnectionFactory.createConnection(cfg);
            admin=connection.getAdmin();
        }catch (Exception e){

        }
    }
    public static void main(String[] args) throws Exception {
        creatHtable();








        connection.close(); // 关闭连接


    }

    public static void creatHtable() throws IOException {
        TableName tableName = TableName.valueOf("user_behavior");
        if (admin.tableExists(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("============= delete " + tableName + " table success ================\n");
        }else {
            System.out.println("=========== the " + tableName + " table does not exist ==============\n");
        }

        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
        // 创建集合用于存放ColumnFamilyDescriptor对象
        List<ColumnFamilyDescriptor> families = new ArrayList<>();

        // 将每个familyName对应的ColumnFamilyDescriptor对象添加到families集合中保存
        // for (String familyName : familyNames)
        families.add(ColumnFamilyDescriptorBuilder.newBuilder("info".getBytes()).build());
        families.add(ColumnFamilyDescriptorBuilder.newBuilder("time".getBytes()).build());

        TableDescriptor build = builder.setColumnFamilies(families).build();

        admin.createTable(build);

        connection.close();
        System.out.println("=============== create " + tableName + " table success ===============\n");
    }

    public static void family() throws IOException{
//        admin.deleteColumnFamily(tn, Bytes.toBytes(familyName));
//        admin.deleteColumnFamily(tn, Bytes.toBytes(familyName));
    }
    public static void write() throws IOException{

        TableName tableName = TableName.valueOf("user_behavior");
        // 设置缓存1m，当达到1m时数据会自动刷到hbase，替代了hTable.setWriteBufferSize(30 * 1024 * 1024)
        BufferedMutatorParams params = new BufferedMutatorParams(tableName);
        params.writeBufferSize(1024 * 1024);
        // 创建一个批量异步与hbase通信的对象
        BufferedMutator mutator = connection.getBufferedMutator(params);
        Put put = new Put(Bytes.toBytes(new Random().hashCode()));
        put.addColumn(Bytes.toBytes("test_family"), Bytes.toBytes("test_col"), Bytes.toBytes(new Random().hashCode()));
        // 向hbase插入数据,达到缓存会自动提交,这里也可以通过传入List<put>的方式批量插入
        mutator.mutate(put);
        // 不用每次put后就调用flush，最后调用就行，这个方法替代了旧api的hTable.setAutoFlush(false, false)
        mutator.flush();
        mutator.close();
    }

}