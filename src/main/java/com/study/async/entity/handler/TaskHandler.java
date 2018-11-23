package com.study.async.entity.handler;

import java.util.concurrent.ExecutorService;

/**
 * Created by lf52 on 2018/11/22.
 *
 * This is the exposed interface which the business component should implement
 * to handle the task .
 */
public interface TaskHandler<T> {
	/**
	 * 自定义的事件处理流程，异步方式
	 * @param tasksource
	 * @param asyncThreadPool
	 */
	public void executeAsync(T tasksource,ExecutorService asyncThreadPool);

	/**
	 * 自定义的事件处理流程，同步方式
	 * @param tasksource
	 */
	public void executeSync(T tasksource);

}
