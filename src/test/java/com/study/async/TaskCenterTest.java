package com.study.async;

import com.study.async.core.task.TaskHandlerCenter;
import com.study.async.entity.TaskSource;
import com.study.async.entity.handler.TaskHandler;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Created by lf52 on 2018/11/23.
 *
 * TaskCenter 使用示例
 */
public class TaskCenterTest {

    List<String> list = new LinkedList<>();
    TaskSource<String> taskSource = new TaskSource<>(list);
    TaskHandlerCenter center = null;

    @Before
    public void Before(){
        for (int i = 0; i <10 ; i++) {
            list.add("leo"+i);
        }
    }
    /**
     * 异步任务模式 ：共享线程池
     */
    @Test
    public void TestAsync4Share(){

        try{
            center = new TaskHandlerCenter(taskSource, new TaskHandler() {
                @Override
                public void executeAsync(Object tasksource,ExecutorService asyncThreadPool) {
                    String source = (String)tasksource;
                    for (int i = 0; i < 2; i++) {
                        asyncThreadPool.submit(new Runnable() {
                            @Override
                            public void run() {
                                System.out.println(Thread.currentThread().getName() + " execute ==== " + source);
                            }
                        });
                    }
                }

                @Override
                public void executeSync(Object tasksource) {
                    //do nothing
                }

            },true,20,10,20);
            center.startup();

            Thread.sleep(5 * 1000);

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            center.shutdownGracefully();
        }

    }

    /**
     * 异步任务模式 ：独立线程池
     */
    @Test
    public void TestAsync4Single(){

        try{
            center = new TaskHandlerCenter(taskSource, new TaskHandler() {
                @Override
                public void executeAsync(Object tasksource,ExecutorService asyncThreadPool) {
                    String source = (String)tasksource;
                    for (int i = 0; i < 2; i++) {
                        asyncThreadPool.submit(new Runnable() {
                            @Override
                            public void run() {
                                System.out.println(Thread.currentThread().getName() + " execute ==== " + source);
                            }
                        });
                    }
                }

                @Override
                public void executeSync(Object tasksource) {
                    //do nothing
                }

            },false,20,10,20);
            center.startup();

            Thread.sleep(5 * 1000);

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            center.shutdownGracefully();
        }

    }

    /**
     * 同步线程模式
     */
    @Test
    public void TestSync(){

        try{
            center =new TaskHandlerCenter(taskSource, new TaskHandler() {
                @Override
                public void executeAsync(Object tasksource, ExecutorService asyncThreadPool) {
                    //do nothing
                }

                @Override
                public void executeSync(Object tasksource) {
                    String source = (String)tasksource;
                    System.out.println(Thread.currentThread().getName() + " execute ==== " + source);
                }

            },10);

            center.startup();

            Thread.sleep(5 * 1000);

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            center.shutdownGracefully();
        }
    }


}
