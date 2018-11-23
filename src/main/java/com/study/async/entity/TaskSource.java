package com.study.async.entity;

import java.util.List;

/**
 * Created by lf52 on 2018/11/22.
 */
public class TaskSource<T> {

    private List<T> t;

    public TaskSource(List<T> t) {
        this.t = t;
    }

    public List<T> getT() {
        return t;
    }

    public void setT(List<T> t) {
        this.t = t;
    }
}
