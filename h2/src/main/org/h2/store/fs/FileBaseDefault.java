/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;

/**
 * Default implementation of the slow operations that need synchronization because they
 * involve the file position.
 */
public abstract class FileBaseDefault extends FileBase {

    private long position = 0;

    @Override
    public final long position() throws IOException {
        Lock channelLock = channelLock();
        channelLock.lock();
        try {
            return position;
        } finally {
            channelLock.unlock();
        }
    }

    @Override
    public final FileChannel position(long newPosition) throws IOException {
        Lock channelLock = channelLock();
        channelLock.lock();
        try {
            if (newPosition < 0) {
                throw new IllegalArgumentException();
            }
            position = newPosition;
            return this;
        } finally {
            channelLock.unlock();
        }
    }

    @Override
    public final int read(ByteBuffer dst) throws IOException {
        Lock channelLock = channelLock();
        channelLock.lock();
        try {
            int read = read(dst, position);
            if (read > 0) {
                position += read;
            }
            return read;
        } finally {
            channelLock.unlock();
        }
    }

    @Override
    public final int write(ByteBuffer src) throws IOException {
        Lock channelLock = channelLock();
        channelLock.lock();
        try {
            int written = write(src, position);
            if (written > 0) {
                position += written;
            }
            return written;
        } finally {
            channelLock.unlock();
        }
    }

    @Override
    public final FileChannel truncate(long newLength) throws IOException {
        Lock channelLock = channelLock();
        channelLock.lock();
        try {
            implTruncate(newLength);
            if (newLength < position) {
                position = newLength;
            }
            return this;
        } finally {
            channelLock.unlock();
        }
    }

    /**
     * The truncate implementation.
     *
     * @param size the new size
     * @throws IOException on failure
     */
    protected abstract void implTruncate(long size) throws IOException;
}
