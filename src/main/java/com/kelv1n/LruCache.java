package com.kelv1n;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class LruCache<K, V> extends ArrowCache<K, V>{
    /*private static final int REMOVE_ALL = -1;
    private static final int DEFAULT_CAPACITY = 10;
    private final Map<K, V> map;
    private final int maxMemorySize;
    private int memorySize;*/

    public LruCache(){
        this(DEFAULT_CAPACITY);
    }

    public LruCache(int capacity){
        super(capacity, true);
    }

    /*@Override
    public final V get(K key){
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
    public final V put(K key, V value){
        Objects.requireNonNull(key, "key == null");
        Objects.requireNonNull(value, "value == null");
        V previous;
        synchronized (this) {
            previous = map.put(key, value);
            memorySize += getValueSize(value);
            if (previous != null) {
                memorySize -= getValueSize(previous);
            }
            timeToSize(maxMemorySize);
        }
        return previous;
    }

    public final V remove(K key){
        Objects.requireNonNull(key, "key ==null");
        V previous;
        synchronized (this){
            previous = map.remove(key);
            if(previous!=null){
                memorySize -= getValueSize(previous);
            }
        }
        return previous;
    }

    protected int getValueSize(V value) {
        return 1;
    }

    public synchronized final void clear(){
        timeToSize(REMOVE_ALL);
    }

    public synchronized final int getMaxMemorySize(){
        return maxMemorySize;
    }

    private synchronized final int getMemorySize(){
        return memorySize;
    }

    public synchronized final Map<K, V> snapshot(){
        return new LinkedHashMap<>(map);
    }*/

    @Override
    public String getClassName(){
        return LruCache.class.getName();
    }

    @Override
    void timeToSize(int maxSize){
        while (true) {
            K key;
            V value;
            if (memorySize <= maxSize || map.isEmpty()) {
                break;
            }
            if (memorySize < 0 || (map.isEmpty() && memorySize != 0)) {
                throw new IllegalStateException(getClassName() + ".getValueSize() is reporting inconsistent results");
            }
            Map.Entry<K, V> toRemove = map.entrySet().iterator().next();
            key = toRemove.getKey();
            value = toRemove.getValue();
            map.remove(key);
            memorySize -= getValueSize(key, value);
        }
    }

}
