package com.study.async.core;

/**
 * Created by lf52 on 2018/11/22.
 */
public abstract class AbstractTask<T> implements Runnable {

    protected T t;

    public AbstractTask(T t){
        this.t = t;
    }


    @Override
    public void run() {
        handleMessage(t);
    }

    protected abstract void handleMessage(T t);

    public abstract void shutdown();

}
