/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.fulltext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.h2.util.SoftValuesHashMap;

/**
 * The global settings of a full text search.
 */
final class FullTextSettings {

    /**
     * The settings of open indexes.
     */
    private static final HashMap<String, FullTextSettings> SETTINGS = new HashMap<>();
    private static final Lock SETTINGS_LOCK = new ReentrantLock();

    /**
     * Whether this instance has been initialized.
     */
    private boolean initialized;

    /**
     * The set of words not to index (stop words).
     */
    private final HashSet<String> ignoreList = new HashSet<>();
    private final Lock ignoreListLock = new ReentrantLock();

    /**
     * The set of words / terms.
     */
    private final HashMap<String, Integer> words = new HashMap<>();
    private final Lock wordsLock = new ReentrantLock();

    /**
     * The set of indexes in this database.
     */
    private final ConcurrentHashMap<Integer, IndexInfo> indexes = new ConcurrentHashMap<>();

    /**
     * The prepared statement cache.
     */
    private final WeakHashMap<Connection, SoftValuesHashMap<String, PreparedStatement>> cache = new WeakHashMap<>();

    /**
     * The whitespace characters.
     */
    private String whitespaceChars = " \t\n\r\f+\"*%&/()=?'!,.;:-_#@|^~`{}[]<>\\";

    private final Lock lock = new ReentrantLock();

    /**
     * Create a new instance.
     */
    private FullTextSettings() {
        // don't allow construction
    }

    /**
     * Clear set of ignored words
     */
    public void clearIgnored() {
        ignoreListLock.lock();
        try {
            ignoreList.clear();
        } finally {
            ignoreListLock.unlock();
        }
    }

    /**
     * Amend set of ignored words
     * @param words to add
     */
    public void addIgnored(Iterable<String> words) {
        ignoreListLock.lock();
        try {
            for (String word : words) {
                word = normalizeWord(word);
                ignoreList.add(word);
            }
        } finally {
            ignoreListLock.unlock();
        }
    }

    /**
     * Clear set of searchable words
     */
    public void clearWordList() {
        wordsLock.lock();
        try {
            words.clear();
        } finally {
            wordsLock.unlock();
        }
    }

    /**
     * Get id for a searchable word
     * @param word to find id for
     * @return Integer id or null if word is not found
     */
    public Integer getWordId(String word) {
        wordsLock.lock();
        try {
            return words.get(word);
        } finally {
            wordsLock.unlock();
        }
    }

    /**
     * Register searchable word
     * @param word to register
     * @param id to register with
     */
    public void addWord(String word, Integer id) {
        wordsLock.lock();
        try {
            words.putIfAbsent(word, id);
        } finally {
            wordsLock.unlock();
        }
    }

    /**
     * Get the index information for the given index id.
     *
     * @param indexId the index id
     * @return the index info
     */
    IndexInfo getIndexInfo(int indexId) {
        return indexes.get(indexId);
    }

    /**
     * Add an index.
     *
     * @param index the index
     */
    void addIndexInfo(IndexInfo index) {
        indexes.put(index.id, index);
    }

    /**
     * Convert a word to uppercase. This method returns null if the word is in
     * the ignore list.
     *
     * @param word the word to convert and check
     * @return the uppercase version of the word or null
     */
    String convertWord(String word) {
        word = normalizeWord(word);
        ignoreListLock.lock();
        try {
            if (ignoreList.contains(word)) {
                return null;
            }
        } finally {
            ignoreListLock.unlock();
        }
        return word;
    }

    /**
     * Get or create the fulltext settings for this database.
     *
     * @param conn the connection
     * @return the settings
     * @throws SQLException on failure
     */
    static FullTextSettings getInstance(Connection conn)
            throws SQLException {
        String path = getIndexPath(conn);
        FullTextSettings setting;
        SETTINGS_LOCK.lock();
        try {
            setting = SETTINGS.get(path);
            if (setting == null) {
                setting = new FullTextSettings();
                SETTINGS.put(path, setting);
            }
        } finally {
            SETTINGS_LOCK.unlock();
        }
        return setting;
    }

    /**
     * Get the file system path.
     *
     * @param conn the connection
     * @return the file system path
     */
    private static String getIndexPath(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(
                "CALL COALESCE(DATABASE_PATH(), 'MEM:' || DATABASE())");
        rs.next();
        String path = rs.getString(1);
        if ("MEM:UNNAMED".equals(path)) {
            throw FullText.throwException(
                    "Fulltext search for private (unnamed) " +
                    "in-memory databases is not supported.");
        }
        rs.close();
        return path;
    }

    /**
     * Prepare a statement. The statement is cached in a soft reference cache.
     *
     * @param conn the connection
     * @param sql the statement
     * @return the prepared statement
     * @throws SQLException on failure
     */
    PreparedStatement prepare(Connection conn, String sql) throws SQLException {
        lock.lock();
        try {
            SoftValuesHashMap<String, PreparedStatement> c = cache.get(conn);
            if (c == null) {
                c = new SoftValuesHashMap<>();
                cache.put(conn, c);
            }
            PreparedStatement prep = c.get(sql);
            if (prep != null && prep.getConnection().isClosed()) {
                prep = null;
            }
            if (prep == null) {
                prep = conn.prepareStatement(sql);
                c.put(sql, prep);
            }
            return prep;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Remove all indexes from the settings.
     */
    void removeAllIndexes() {
        indexes.clear();
    }

    /**
     * Remove an index from the settings.
     *
     * @param index the index to remove
     */
    void removeIndexInfo(IndexInfo index) {
        indexes.remove(index.id);
    }

    /**
     * Set the initialized flag.
     *
     * @param b the new value
     */
    void setInitialized(boolean b) {
        this.initialized = b;
    }

    /**
     * Get the initialized flag.
     *
     * @return whether this instance is initialized
     */
    boolean isInitialized() {
        return initialized;
    }

    /**
     * Close all fulltext settings, freeing up memory.
     */
    static void closeAll() {
        SETTINGS_LOCK.lock();
        try {
            SETTINGS.clear();
        } finally {
            SETTINGS_LOCK.unlock();
        }
    }

    void setWhitespaceChars(String whitespaceChars) {
        this.whitespaceChars = whitespaceChars;
    }

    String getWhitespaceChars() {
        return whitespaceChars;
    }

    private static String normalizeWord(String word) {
        // TODO this is locale specific, document
        return word.toUpperCase();
    }
}
