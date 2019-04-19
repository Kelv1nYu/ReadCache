package com.kelv1n;

public interface Cache<K, V>{

    boolean isExists(K key);

    /**
     *
     * @param key
     * @param value
     */
    V put(K key, V value);

    /**
     *
     * @param key
     * @return
     */
    V get(K key);

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
    V remove(K key);

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
