package com.kelv1n;

import java.util.*;

public class LfuCache<K, V> extends ArrowCache<K, V>{
    /*private static final int REMOVE_ALL = -1;
    private static final int DEFAULT_CAPACITY = 10;
    private final Map<K, V> map;
    Map<K, HitRate> hitRateMap = new HashMap<K, HitRate>();
    private final int maxMemorySize;
    private int memorySize;*/
    Map<K, HitRate> hitRateMap = new HashMap<K, HitRate>();


    public LfuCache(){
        this(DEFAULT_CAPACITY);
    }

    public LfuCache(int capacity){
        /*map = new LinkedHashMap(capacity, 0.75f, false);
        maxMemorySize = capacity * 1024 * 1024;*/
        super(capacity, false);
    }

    @Override
    public final V get(K key){
        Objects.requireNonNull(key, "key == null");
        synchronized (this){
            V v = map.get(key);
            if(v != null){
                HitRate hitRate = hitRateMap.get(key);
                hitRate.hitCount += 1;
                hitRate.lastTime = System.nanoTime();
                return v;
            }
        }
        return null;
    }

    @Override
    public final V put(K key, V value){
        Objects.requireNonNull(key, "key == null");
        Objects.requireNonNull(value, "value == null");
        V previous;
        synchronized (this){
            while(hitRateMap.size() >= capacity){
                K k = getRemovedKey();
                hitRateMap.remove(k);
                previous = map.remove(k);
                memorySize -= getValueSize(key, previous);
            }
            previous = map.put(key, value);
            hitRateMap.put(key, new HitRate(key, 1, System.nanoTime()));
            memorySize += getValueSize(key, value);
            if(previous != null){
                memorySize -= getValueSize(key, previous);
            }
            //timeToSize(maxMemorySize);
        }
        return previous;
    }

    @Override
    public final V remove(K key){
        Objects.requireNonNull(key, "key ==null");
        V previous;
        synchronized (this){
            previous = map.remove(key);
            hitRateMap.remove(key);
            if(previous!=null){
                memorySize -= getValueSize(key, previous);
            }
        }
        return previous;
    }

    /*protected int getValueSize(V value) {
        return 1;
    }*/

    private synchronized final K getRemovedKey(){
        HitRate min = Collections.min(hitRateMap.values());
        return min.key;
    }

    /*public synchronized final void clear(){
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

    public String getClassName(){
        return LfuCache.class.getName();
    }

    @Override
    void timeToSize(int maxSize){
        while(true){
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
            hitRateMap.remove(key);
            memorySize -= getValueSize(key, value);
        }
    }

    class HitRate implements Comparable<HitRate>{
        K key;
        Integer hitCount;
        Long lastTime;

        public HitRate(K key, Integer hitCount, Long lastTime){
            this.key = key;
            this.hitCount = hitCount;
            this.lastTime = lastTime;
        }

        @Override
        public int compareTo(HitRate o){
            int hr = hitCount.compareTo(o.hitCount);
            return hr != 0 ? hr : lastTime.compareTo(o.lastTime);
        }
    }
}
