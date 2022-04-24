package com.fyc.source;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class MysqlSource extends RichSourceFunction {


    @Override
    public void run(SourceContext sourceContext) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
