package com.kelv1n;

public interface KVCache {
    boolean isExists(Object key);

    /**
     *
     * @param key
     * @param value
     */
    Object put(Object key, Object value);

    /**
     *
     * @param key
     * @return
     */
    Object get(Object key);

    /**
     *
     * @param startSeqNum
     * @param num
     * @return
     */
    //Object getDataFromSegment(long startSeqNum, int num);

    /**
     *
     * @param key
     * @return
     */
    Object remove(Object key);

    /**
     *
     */
    void clear();

    /**
     *
     * @return
     */
    int getSize();
}
