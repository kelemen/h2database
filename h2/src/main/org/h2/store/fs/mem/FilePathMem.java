/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs.mem;

import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.store.fs.FilePath;

/**
 * This file system keeps files fully in memory. There is an option to compress
 * file blocks to save memory.
 */
public class FilePathMem extends FilePath {

    private static final TreeMap<String, FileMemData> MEMORY_FILES =
            new TreeMap<>();
    private static final Lock MEMORY_FILES_LOCK = new ReentrantLock();
    private static final FileMemData DIRECTORY = new FileMemData("", false);

    @Override
    public FilePathMem getPath(String path) {
        FilePathMem p = new FilePathMem();
        p.name = getCanonicalPath(path);
        return p;
    }

    @Override
    public long size() {
        return getMemoryFile().length();
    }

    @Override
    public void moveTo(FilePath newName, boolean atomicReplace) {
        MEMORY_FILES_LOCK.lock();
        try {
            if (!atomicReplace && !newName.name.equals(name) &&
                    MEMORY_FILES.containsKey(newName.name)) {
                throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2, name, newName + " (exists)");
            }
            FileMemData f = getMemoryFile();
            f.setName(newName.name);
            MEMORY_FILES.remove(name);
            MEMORY_FILES.put(newName.name, f);
        } finally {
            MEMORY_FILES_LOCK.unlock();
        }
    }

    @Override
    public boolean createFile() {
        MEMORY_FILES_LOCK.lock();
        try {
            if (exists()) {
                return false;
            }
            getMemoryFile();
        } finally {
            MEMORY_FILES_LOCK.unlock();
        }
        return true;
    }

    @Override
    public boolean exists() {
        if (isRoot()) {
            return true;
        }
        MEMORY_FILES_LOCK.lock();
        try {
            return MEMORY_FILES.get(name) != null;
        } finally {
            MEMORY_FILES_LOCK.unlock();
        }
    }

    @Override
    public void delete() {
        if (isRoot()) {
            return;
        }
        MEMORY_FILES_LOCK.lock();
        try {
            FileMemData old = MEMORY_FILES.remove(name);
            if (old != null) {
                old.truncate(0);
            }
        } finally {
            MEMORY_FILES_LOCK.unlock();
        }
    }

    @Override
    public List<FilePath> newDirectoryStream() {
        ArrayList<FilePath> list = new ArrayList<>();
        MEMORY_FILES_LOCK.lock();
        try {
            for (String n : MEMORY_FILES.tailMap(name).keySet()) {
                if (n.startsWith(name)) {
                    if (!n.equals(name) && n.indexOf('/', name.length() + 1) < 0) {
                        list.add(getPath(n));
                    }
                } else {
                    break;
                }
            }
            return list;
        } finally {
            MEMORY_FILES_LOCK.unlock();
        }
    }

    @Override
    public boolean setReadOnly() {
        return getMemoryFile().setReadOnly();
    }

    @Override
    public boolean canWrite() {
        return getMemoryFile().canWrite();
    }

    @Override
    public FilePathMem getParent() {
        int idx = name.lastIndexOf('/');
        return idx < 0 ? null : getPath(name.substring(0, idx));
    }

    @Override
    public boolean isDirectory() {
        if (isRoot()) {
            return true;
        }
        MEMORY_FILES_LOCK.lock();
        try {
            FileMemData d = MEMORY_FILES.get(name);
            return d == DIRECTORY;
        } finally {
            MEMORY_FILES_LOCK.unlock();
        }
    }

    @Override
    public boolean isRegularFile() {
        if (isRoot()) {
            return false;
        }
        MEMORY_FILES_LOCK.lock();
        try {
            FileMemData d = MEMORY_FILES.get(name);
            return d != null && d != DIRECTORY;
        } finally {
            MEMORY_FILES_LOCK.unlock();
        }
    }

    @Override
    public boolean isAbsolute() {
        // TODO relative files are not supported
        return true;
    }

    @Override
    public FilePathMem toRealPath() {
        return this;
    }

    @Override
    public long lastModified() {
        return getMemoryFile().getLastModified();
    }

    @Override
    public void createDirectory() {
        if (exists()) {
            throw DbException.get(ErrorCode.FILE_CREATION_FAILED_1,
                    name + " (a file with this name already exists)");
        }
        MEMORY_FILES_LOCK.lock();
        try {
            MEMORY_FILES.put(name, DIRECTORY);
        } finally {
            MEMORY_FILES_LOCK.unlock();
        }
    }

    @Override
    public FileChannel open(String mode) {
        FileMemData obj = getMemoryFile();
        return new FileMem(obj, "r".equals(mode));
    }

    private FileMemData getMemoryFile() {
        MEMORY_FILES_LOCK.lock();
        try {
            FileMemData m = MEMORY_FILES.get(name);
            if (m == DIRECTORY) {
                throw DbException.get(ErrorCode.FILE_CREATION_FAILED_1,
                        name + " (a directory with this name already exists)");
            }
            if (m == null) {
                m = new FileMemData(name, compressed());
                MEMORY_FILES.put(name, m);
            }
            return m;
        } finally {
            MEMORY_FILES_LOCK.unlock();
        }
    }

    private boolean isRoot() {
        return name.equals(getScheme() + ":");
    }

    /**
     * Get the canonical path for this file name.
     *
     * @param fileName the file name
     * @return the canonical path
     */
    protected static String getCanonicalPath(String fileName) {
        fileName = fileName.replace('\\', '/');
        int idx = fileName.indexOf(':') + 1;
        if (fileName.length() > idx && fileName.charAt(idx) != '/') {
            fileName = fileName.substring(0, idx) + "/" + fileName.substring(idx);
        }
        return fileName;
    }

    @Override
    public String getScheme() {
        return "memFS";
    }

    /**
     * Whether the file should be compressed.
     *
     * @return if it should be compressed.
     */
    boolean compressed() {
        return false;
    }

}


