package com.patrick.thread;


/**
 * 死锁类型：
 * 数据库死锁，线程死锁，行锁，表锁
 * <p>
 * 线程死锁原因：同步中嵌套同步
 */


class ThreadTrain5 implements Runnable {
    private static int count = 100;
    private Object obj = new Object();
    public boolean flag = true;

    @Override
    public void run() {
        if (flag) {
            while (count > 0) {
                synchronized (obj) {
                    show();
                }
            }
        } else {
            while (count > 0) {
                show();
            }
        }
    }

    public synchronized void show() {
        synchronized (obj) {
            if (count > 0) {
                try {
                    Thread.sleep(40);
                } catch (Exception e) {

                }

                System.out.println(Thread.currentThread().getName() + ", sell ticker:" + (100 - count + 1));
                count--;

            }
        }
    }
}

public class Deadlock01 {
    public static void main(String[] args) {

    }
}
