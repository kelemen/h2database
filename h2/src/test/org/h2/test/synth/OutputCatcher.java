/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.h2.util.IOUtils;

/**
 * Catches the output of another process.
 */
public class OutputCatcher extends Thread {
    private final InputStream in;
    private final LinkedList<String> list = new LinkedList<>();
    private final Lock listLock = new ReentrantLock();
    private final Condition listCondition = listLock.newCondition();

    public OutputCatcher(InputStream in) {
        this.in = in;
    }

    /**
     * Read a line from the output.
     *
     * @param wait the maximum number of milliseconds to wait
     * @return the line
     */
    public String readLine(long wait) {
        long start = System.nanoTime();
        while (true) {
            listLock.lock();
            try {
                if (list.size() > 0) {
                    return list.removeFirst();
                }
                try {
                    listCondition.await(wait, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    // ignore
                }
                long time = System.nanoTime() - start;
                if (time >= TimeUnit.MILLISECONDS.toNanos(wait)) {
                    return null;
                }
            } finally {
                listLock.unlock();
            }
        }
    }

    @Override
    public void run() {
        final StringBuilder buff = new StringBuilder();
        try {
            while (true) {
                try {
                    int x = in.read();
                    if (x < 0) {
                        break;
                    }
                    if (x < ' ') {
                        if (buff.length() > 0) {
                            String s = buff.toString();
                            buff.setLength(0);
                            listLock.lock();
                            try {
                                list.add(s);
                                listCondition.signalAll();
                            } finally {
                                listLock.unlock();
                            }
                        }
                    } else {
                        buff.append((char) x);
                    }
                } catch (IOException e) {
                    break;
                }
            }
            IOUtils.closeSilently(in);
        } finally {
            // just in case something goes wrong, make sure we store any partial output we got
            if (buff.length() > 0) {
                listLock.lock();
                try {
                    list.add(buff.toString());
                    listCondition.signalAll();
                } finally {
                    listLock.unlock();
                }
            }
        }
    }
}
