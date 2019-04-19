package com.kelv1n;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;
import sun.misc.LRUCache;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.*;

public class ArrowCacheTest {

    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    IntVector intVector = new IntVector("test-intVector", allocator);
    ValueVector valueVector;
    int currentCountOfEvents = 0;
    LruCache<Long, ByteBuffer> lruCache;
    Long l1 = 1L;

    private static ArrowCache cache;
    private LinkedHashMap<Integer, Integer> map = null;
    ArrowCacheHandle handle;
    //public ReplaceType type = ReplaceType.RANDOM;


    public ArrowCacheTest(){
        handle = new ArrowCacheHandle(ReplaceType.LRU, 3);
    }

    /*@Test
    public void testPut(){
        //handle.testType();
        *//*if(cache.fifoCache!=null){
            System.out.println(cache.fifoCache.getClassName());
        }
        if(cache.lfuCache!=null){
            System.out.println(cache.lfuCache.getClassName());
        }
        if(cache.lruCache!=null){
            System.out.println(cache.lruCache.getClassName());
        }
        if(cache.randomCache!=null){
            System.out.println(cache.randomCache.getClassName());
        }
*//*


        *//*cache.putData(1, 1);
        cache.putData(2, 2);
        cache.putData(3, 3);
        cache.getData(1);
        cache.putData(4, 4);
        System.out.println(cache.getData(1));
        System.out.println(cache.getData(2));
        System.out.println(cache.getData(3));
        System.out.println(cache.getData(4));*//*

        *//*map = new LinkedHashMap<Integer, Integer>(3, 0.75f, false){//第三个参数即为LinkedHashMap中的accessOrder true：将按照访问顺序（如果已经存在将其插入末尾）； false：按照插入数序（再次插入不影响顺序）
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer,Integer> eldest) {//重写删除最早的entry
                return size() > 3;//如果条件当前size大于cap，就删除最早的（返回true）
            }
        };

        map.put(1,1);
        System.out.println(map);
        map.put(2,2);
        System.out.println(map);
        map.put(3,3);
        System.out.println(map);
        map.put(1,1);
        System.out.println(map);
        map.get(1);
        System.out.println(map);
        map.get(3);
        System.out.println(map);
        map.put(4,4);
        System.out.println(map);*//*
        *//*final Random random = new Random();

        map = new LinkedHashMap<Integer, Integer>(3, 0.75f, false){
            @Override
            public Integer put(Integer key, Integer value){
                if(map.size() > 3){
                    map.remove(random.nextInt(3)+1);
                    map.put(key, value);
                }else{
                    map.put(key, value);
                }
                return value;
            }
        };
        map.put(1,1);
        System.out.println(map);
        map.put(2,2);
        System.out.println(map);
        map.put(3,3);
        System.out.println(map);
        map.put(1,1);
        System.out.println(map);
        map.get(1);
        System.out.println(map);
        map.get(3);
        System.out.println(map);
        map.put(4,4);
        System.out.println(map);*//*
    }*/

    @Test
    public void testWrite(){
        handle.testWrite();

        /*List<Field> fields = new ArrayList<Field>();
        List<FieldVector> fieldVectors = new ArrayList<FieldVector>();


        fields.add(intVector.getField());
        fieldVectors.add(intVector);
        VectorSchemaRoot schemaRoot = new VectorSchemaRoot(fields, fieldVectors, 0);
        IntVectorWriter intVectorWriter = new IntVectorWriter(schemaRoot, intVector);

        for (int i = 0; i < 10; i++) {
            intVectorWriter.writeArrow(i, 0);
        }*/

        //intVectorWriter.writeArrow(-1, 1);
        //intVector.setValueCount(0);
        //intVector.setSafe(currentCountOfEvents, 1);
        //valueVector.setValueCount(++currentCountOfEvents);
        //schemaRoot.setRowCount(1);

        //ArrowRecordBatch batch = new VectorUnloader(schemaRoot).getRecordBatch();
        //ByteBuffer data = ArrowSerializer.serialize(batch);

        //lruCache.put(l1,data);

        //System.out.println(lruCache.isExists(l1));

        //

        //
        //data.flip();

        //System.out.println(data);



    }

    @Test
    public void testRead(){
        handle.testRead();
        //ByteBuffer readData = lruCache.get(l1);
        //System.out.println(readData);
        //ArrowRecordBatch recordBatch = null;
        //recordBatch = ArrowSerializer.deserializeRecordBatch(lruCache.get(l1), allocator);

    }

}