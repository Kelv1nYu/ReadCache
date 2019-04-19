package com.kelv1n;

import java.util.Locale;

public enum ReplaceType {
    /**
     * First in first out
     */
    FIFO,

    /**
     * Least Recently Used
     */
    LRU,

    /**
     * Least Frequently Used
     */
    LFU,

    /**
     * Random replace
     */
    RANDOM,

    /**
     * LRU + LFU with special weight
     */
    LRFU,

    /**
     * Null model
     */
    NULL;

    private final String name;

    ReplaceType(){
        this.name = this.name().toLowerCase(Locale.ROOT);
    }

    public String getName(){
        return name;
    }
}
