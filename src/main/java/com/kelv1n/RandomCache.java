package com.kelv1n;

import java.util.*;

public class RandomCache<K, V> extends ArrowCache<K, V>{
    /*private static final int REMOVE_ALL = -1;
    private static final int DEFAULT_CAPACITY = 10;
    private final Map<K, V> map;
    private final int maxMemorySize;
    private int memorySize;*/

    public RandomCache(){
        this(DEFAULT_CAPACITY);
    }

    public RandomCache(int capacity){
        /*map = new LinkedHashMap(capacity, 0.75f, false);
        maxMemorySize = capacity * 1024 * 1024;*/
        super(capacity, false);
    }

    /*public final V get(K key){
        Objects.requireNonNull(key, "key == null");
        synchronized (this){
            V v = map.get(key);
            if(v != null){
                return v;
            }
        }
        return null;
    }

    public final V put(K key, V value){
        Objects.requireNonNull(key, "key == null");
        Objects.requireNonNull(value, "value == null");
        V previous;
        synchronized (this){
            previous = map.put(key, value);
            memorySize += getValueSize(value);
            if(previous != null){
                memorySize -= getValueSize(previous);
            }
            timeToSize(maxMemorySize);
        }
        return previous;
    }

    public final V remove(Object key){
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
        return RandomCache.class.getName();
    }

    @Override
    void timeToSize(int maxSize){
        Random random = new Random();
        int r;
        while(true){
            if (memorySize <= maxSize || map.isEmpty()) {
                break;
            }
            if (memorySize < 0 || (map.isEmpty() && memorySize != 0)) {
                throw new IllegalStateException(getClassName() + ".getValueSize() is reporting inconsistent results");
            }

            if(maxSize == -1){
                K k = map.keySet().iterator().next();
                V v = map.remove(k);
                memorySize -= getValueSize(k, v);
            }else{
                r = random.nextInt(capacity) + 1;
                //System.out.println(r);
                int i = 1;
                for (K key : map.keySet()) {
                    if (i == r) {
                        V v = map.remove(key);
                        memorySize -= getValueSize(key, v);
                        break;
                    } else {
                        i++;
                    }
                }
            }

        }
    }
}