package com.study.async.kafka;

import com.study.async.core.task.TaskHandlerCenter;
import com.study.async.entity.TaskSource;
import com.study.async.entity.handler.KafkaTaskHandler;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class KConsumerClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(KConsumerClient.class);

    private List<String> subscribe;
    private Properties props;

    private TaskHandlerCenter center = null;
    List<KafkaConsumer<byte[], byte[]>> consumerlist = null;

    private int taskThreadNum =  Runtime.getRuntime().availableProcessors();
    private int threadNum = 0;
    private int minThreadNum = 0;
    private int maxThreadNum = 0;

    private boolean isAsyncThreadModel = false;
    private boolean isSharedAsyncThreadPool = false;

    public KConsumerClient(List<String> subscribe, Properties props, boolean isSharedAsyncThreadPool, int threadNum, int minThreadNum, int maxThreadNum){
        this.subscribe = subscribe;
        this.props = props;
        this.threadNum = threadNum;
        this.minThreadNum = minThreadNum;
        this.maxThreadNum = maxThreadNum;
        this.isAsyncThreadModel = true;
        this.isSharedAsyncThreadPool = isSharedAsyncThreadPool;
    }

    public KConsumerClient(List<String> subscribe, Properties props,int taskThreadNum){
        this.subscribe = subscribe;
        this.props = props;
        this.taskThreadNum = taskThreadNum;
    }

    public void consumer() {
        consumer(1);
    }

    /**
     * @param consumerNum consumer 线程数目
     */
    public void consumer(int consumerNum){

        consumerlist = new ArrayList<>(consumerNum);
        for (int i = 0; i < consumerNum; i++) {
            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer(props);
            consumer.subscribe(subscribe);
            consumerlist.add(consumer);
        }

        TaskSource<KafkaConsumer<byte[], byte[]>> tasksource = new TaskSource<>(consumerlist);
        KafkaTaskHandler handler = new KafkaTaskHandler();

        if(isAsyncThreadModel){
            center = new TaskHandlerCenter(tasksource,handler,isSharedAsyncThreadPool,threadNum,minThreadNum,maxThreadNum);
        }else{
            center = new TaskHandlerCenter(tasksource,handler,taskThreadNum);
        }

        center.startup();

    }

    public void close() {
        consumerlist.forEach(consumer->{
            if(consumer != null ){
                consumer.close();
            }
        });
        center.shutdownGracefully();
    }

}
