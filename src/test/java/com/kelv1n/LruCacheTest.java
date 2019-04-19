package com.kelv1n;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class LruCacheTest {

    private static LruCache<Long, ByteBuffer> cache;

    @BeforeClass
    public static void createCache(){
        cache = new LruCache<>(3);
    }

    @Test
    public void testPut(){
        long seqNum = 1;
        ByteBuffer testData = ByteBuffer.allocate(20);
        testData.putInt(1);
        testData.flip();

        cache.put(seqNum, testData);
        assertEquals(true, cache.isExists(seqNum));
    }

    @Test
    public void testGet(){
        long seqNum = 1;
        ByteBuffer testData = ByteBuffer.allocate(20);
        testData.putInt(1);
        testData.flip();

        cache.put(seqNum, testData);
        assertEquals(true, cache.isExists(seqNum));


        ByteBuffer readableData = cache.get(seqNum);
        assertEquals(1, readableData.getInt());
    }

    @Test
    public void testRemove(){
        long seqNum = 1;
        ByteBuffer testData = ByteBuffer.allocate(20);
        testData.putInt(1);
        testData.flip();

        cache.put(seqNum, testData);
        assertEquals(true, cache.isExists(seqNum));


        cache.remove(seqNum);
        assertEquals(false, cache.isExists(seqNum));
    }

    @Test
    public void testExist(){
        long seqNum = 1;
        long seqNum2 = 2;
        ByteBuffer testData = ByteBuffer.allocate(20);
        testData.putInt(1);
        testData.flip();

        cache.put(seqNum, testData);
        assertEquals(true, cache.isExists(seqNum));
        assertEquals(false, cache.isExists(seqNum2));

    }

    @Test
    public void clear(){
        long seqNum = 1;
        long seqNum2 = 2;
        ByteBuffer testData = ByteBuffer.allocate(20);
        testData.putInt(1);
        testData.flip();

        cache.put(seqNum, testData);
        assertEquals(true, cache.isExists(seqNum));

        cache.put(seqNum2, testData);
        assertEquals(true, cache.isExists(seqNum2));

        cache.clear();
        assertEquals(0, cache.getSize());
        assertEquals(false, cache.isExists(seqNum));
        assertEquals(false, cache.isExists(seqNum2));
    }

    @Test
    public void testGetSize(){
        long seqNum = 1;
        long seqNum2 = 2;
        long seqNum3 = 3;
        long seqNum4 = 4;
        ByteBuffer testData = ByteBuffer.allocate(20);
        testData.putInt(1);
        testData.flip();

        cache.put(seqNum, testData);
        assertEquals(true, cache.isExists(seqNum));
        assertEquals(1, cache.getSize());

        cache.put(seqNum2, testData);
        assertEquals(true, cache.isExists(seqNum2));
        assertEquals(2, cache.getSize());

        cache.put(seqNum3, testData);
        assertEquals(true, cache.isExists(seqNum3));
        assertEquals(3, cache.getSize());

        cache.put(seqNum4, testData);
        assertEquals(true, cache.isExists(seqNum4));
        assertEquals(3, cache.getSize());

        cache.clear();
        assertEquals(0, cache.getSize());
    }

    @Test
    public void testLru(){
        long seqNum = 1;
        long seqNum2 = 2;
        long seqNum3 = 3;
        long seqNum4 = 4;
        ByteBuffer testData = ByteBuffer.allocate(20);
        testData.putInt(1);
        testData.flip();

        cache.put(seqNum, testData);
        assertEquals(true, cache.isExists(seqNum));
        assertEquals(1, cache.getSize());

        cache.put(seqNum2, testData);
        assertEquals(true, cache.isExists(seqNum));
        assertEquals(true, cache.isExists(seqNum2));
        assertEquals(2, cache.getSize());

        cache.put(seqNum3, testData);
        assertEquals(true, cache.isExists(seqNum));
        assertEquals(true, cache.isExists(seqNum2));
        assertEquals(true, cache.isExists(seqNum3));
        assertEquals(3, cache.getSize());

        cache.put(seqNum4, testData);
        assertEquals(false, cache.isExists(seqNum));
        assertEquals(true, cache.isExists(seqNum2));
        assertEquals(true, cache.isExists(seqNum3));
        assertEquals(true, cache.isExists(seqNum4));
        assertEquals(3, cache.getSize());

        ByteBuffer readableData = cache.get(seqNum2);
        assertEquals(1, readableData.getInt());

        cache.put(seqNum, testData);
        assertEquals(true, cache.isExists(seqNum));
        assertEquals(true, cache.isExists(seqNum2));
        assertEquals(false, cache.isExists(seqNum3));
        assertEquals(true, cache.isExists(seqNum4));
        assertEquals(3, cache.getSize());

        cache.clear();
        assertEquals(0, cache.getSize());

    }




}