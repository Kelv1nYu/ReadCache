package com.kelv1n;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

public abstract class ArrowCache<K, V> implements Cache<K, V>{

    protected static final int REMOVE_ALL = -1;
    protected static final int DEFAULT_CAPACITY = 10;
    protected final Map<K, V> map;
    protected final int maxMemorySize;
    protected int memorySize;
    protected int capacity;

    //private LinkedHashMap<byte[], byte[]> cache = null;//用LinkedHashMap实现
    //private LinkedHashMap<Integer, Integer> cache = null;
    //private boolean accessOrder = false;
    //private int maxCacheSize;
    //private static ReplaceType type = ReplaceType.NULL;
    //private static ArrowCacheWrapper cache = null;
    /*LfuCache<Integer, ByteBuffer> lfuCache;
    FIFOCache<Integer, ByteBuffer> fifoCache;
    LruCache<Integer, ByteBuffer> lruCache;
    RandomCache<Integer, ByteBuffer> randomCache;*/

    public ArrowCache(){
        this(DEFAULT_CAPACITY, false);
    }

    public ArrowCache(final int capacity, boolean accessOrder){
        if (capacity <= 0){
            throw new IllegalArgumentException("capacity <= 0");
        }
        map = new BasedLinkedHashMap<>(capacity, accessOrder);
        maxMemorySize = capacity * 1024 * 1024;
        this.capacity = capacity;

        /*switch (type){
            case LFU:
                 lfuCache = new LfuCache<>(maxCacheSize);
                 break;
            case FIFO:
                fifoCache = new FIFOCache<>(maxCacheSize);
                break;
            case LRU:
                lruCache = new LruCache<>(maxCacheSize);
                break;
            case RANDOM:
                randomCache = new RandomCache<>(maxCacheSize);
        }*/

    }

    @Override
    public boolean isExists(K key){
        return map.containsKey(key);
    }

    @Override
    public V put(K key, V value){
        Objects.requireNonNull(key, "key == null");
        Objects.requireNonNull(value, "value == null");
        V previous;
        synchronized (this) {
            previous = map.put(key, value);
            memorySize += getValueSize(key, value);
            if (previous != null) {
                memorySize -= getValueSize(key, previous);
            }
            timeToSize(capacity);
        }
        return previous;
    }

    @Override
    public V get(K key){
        Objects.requireNonNull(key, "key == null");
        synchronized (this){
            V value = map.get(key);
            if(value != null){
                return value;
            }
        }
        return null;

    }

    @Override
    public V remove(K key){
        Objects.requireNonNull(key, "key ==null");
        V previous;
        synchronized (this){
            previous = map.remove(key);
            if(previous!=null){
                memorySize -= getValueSize(key, previous);
            }
        }
        return previous;
    }

    @Override
    public void clear(){
        timeToSize(REMOVE_ALL);
    }

    @Override
    public int getSize(){
        return memorySize;
    }

    /*protected int getValueSize(V value) {
        return 1;
    }*/

    abstract void timeToSize(int maxSize);

    protected String getClassName(){
        return ArrowCache.class.getName();
    }

    protected int sizeOf(K key, V value){
        return 1;
    }

    protected int getValueSize(K key, V value){
        int result = sizeOf(key, value);
        if(result < 0 ){
            throw new IllegalStateException("Negative size:" + key + "=" + value);
        }
        return result;
    }

    public synchronized final Map<K, V> snapshot(){
        return new LinkedHashMap<>(map);
    }

}
