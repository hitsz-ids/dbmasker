package com.dbmasker.utils;

import com.dbmasker.api.DBSecManager;

/**
 * The Config class uses the Singleton pattern to manage global configuration.
 */
public class Config {

    // The single instance of Config
    private static Config config;


    // The size of the data
    private int dataSize;

    // Whether to handle rename
    private boolean handleRename;

    /**
     * Private constructor initializes dataSize to DBSecManager.MATCH_DATA_SIZE.
     * This constructor is private as we want to restrict the instantiation of Config to only through getInstance method.
     */
    private Config() {
        dataSize = DBSecManager.MATCH_DATA_SIZE;
        handleRename = true;
    }

    /**
     * Returns the single instance of Config.
     * If the instance has not been created, it creates a new one.
     * Note: This method is thread-safe.
     *
     * @return The single instance of Config
     */
    public static synchronized Config getInstance() {
        if (config == null) {
            config = new Config();
        }
        return config;
    }

    /**
     * Returns the size of the data.
     *
     * @return The size of the data
     */
    public int getDataSize() {
        return dataSize;
    }

    /**
     * Sets the size of the data.
     *
     * @param dataSize The new size of the data
     */
    public void setDataSize(int dataSize) {
        this.dataSize = dataSize;
    }

    /**
     * Returns whether to handle rename.
     *
     * @return Whether to handle rename
     */
    public boolean getHandleRename() {
        return handleRename;
    }

    /**
     * Sets whether to handle rename.
     *
     * @param handleRename Whether to handle rename
     */
    public void setHandleRename(boolean handleRename) {
        this.handleRename = handleRename;
    }
}

