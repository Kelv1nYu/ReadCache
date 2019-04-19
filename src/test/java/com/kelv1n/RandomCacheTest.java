package com.kelv1n;

import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class RandomCacheTest {
    private static RandomCache<Long, ByteBuffer> cache;
    long seqNum = 1;
    long seqNum2 = 2;
    long seqNum3 = 3;
    long seqNum4 = 4;
    long seqNum5 = 5;
    long seqNum6 = 6;

    @BeforeClass
    public static void createCache(){
        cache = new RandomCache<>(5);
    }

    @Test
    public void testPut(){
        ByteBuffer testData = ByteBuffer.allocate(20);
        testData.putInt(1);
        testData.flip();

        cache.put(seqNum, testData);
        assertEquals(true, cache.isExists(seqNum));
    }

    @Test
    public void testGet(){
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
        ByteBuffer testData = ByteBuffer.allocate(20);
        testData.putInt(1);
        testData.flip();

        cache.put(seqNum, testData);
        assertEquals(true, cache.isExists(seqNum));
        assertEquals(false, cache.isExists(seqNum2));

    }

    @Test
    public void clear(){
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
        assertEquals(4, cache.getSize());

        cache.put(seqNum5, testData);
        assertEquals(true, cache.isExists(seqNum5));
        assertEquals(5, cache.getSize());

        cache.put(seqNum6, testData);
        assertEquals(true, cache.isExists(seqNum6));
        assertEquals(5, cache.getSize());

        cache.clear();
        assertEquals(0, cache.getSize());
    }

    @Test
    public void testRandom(){
        ByteBuffer testData = ByteBuffer.allocate(20);
        testData.putInt(1);
        testData.flip();

        cache.put(seqNum, testData);
        assertEquals(1, cache.getSize());

        cache.put(seqNum2, testData);
        assertEquals(2, cache.getSize());

        cache.put(seqNum3, testData);
        assertEquals(3, cache.getSize());

        cache.put(seqNum4, testData);
        assertEquals(4, cache.getSize());


        cache.put(seqNum5, testData);
        assertEquals(5, cache.getSize());

        cache.put(seqNum6, testData);
        System.out.println(cache.isExists(seqNum));
        System.out.println(cache.isExists(seqNum2));
        System.out.println(cache.isExists(seqNum3));
        System.out.println(cache.isExists(seqNum4));
        System.out.println(cache.isExists(seqNum5));
        System.out.println(cache.isExists(seqNum6));
        assertEquals(5, cache.getSize());

        cache.clear();
        assertEquals(0, cache.getSize());

    }

}