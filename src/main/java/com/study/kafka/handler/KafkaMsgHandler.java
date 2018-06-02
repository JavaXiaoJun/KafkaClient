package com.study.kafka.handler;

public interface KafkaMsgHandler<T> {

    /**
     * read message handler
     * @param t
     * @return
     * @throws Exception
     */
    Boolean callback(T t) throws Exception;

}
