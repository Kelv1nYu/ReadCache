package com.kelv1n;

import java.util.LinkedHashMap;
import java.util.Map;

public class BasedLinkedHashMap<K, V> extends LinkedHashMap<K, V> {
    private int capacity;

    public BasedLinkedHashMap(int capacity, boolean accessOrder){
        super(capacity, 0.75f, accessOrder);
        this.capacity = capacity;
    }

    /*@Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest){
        return size() > capacity;
    }*/
}
