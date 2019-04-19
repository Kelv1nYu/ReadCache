package com.kelv1n;

import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class LfuCacheTest {
    private static LfuCache<Long, ByteBuffer> cache;

    @BeforeClass
    public static void createCache(){
        cache = new LfuCache<>(3);
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
    public void testLfu(){
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
        readableData.flip();

        cache.put(seqNum, testData);
        assertEquals(true, cache.isExists(seqNum));
        assertEquals(true, cache.isExists(seqNum2));
        assertEquals(false, cache.isExists(seqNum3));
        assertEquals(true, cache.isExists(seqNum4));
        assertEquals(3, cache.getSize());

        ByteBuffer readableData2 = cache.get(seqNum2);
        assertEquals(1, readableData2.getInt());
        readableData2.flip();

        ByteBuffer readableData3 = cache.get(seqNum2);
        assertEquals(1, readableData3.getInt());
        readableData3.flip();

        ByteBuffer readableData4 = cache.get(seqNum4);
        assertEquals(1, readableData4.getInt());
        readableData4.flip();

        ByteBuffer readableData5 = cache.get(seqNum);
        assertEquals(1, readableData5.getInt());
        readableData5.flip();

        cache.put(seqNum3, testData);
        assertEquals(true, cache.isExists(seqNum));
        assertEquals(true, cache.isExists(seqNum2));
        assertEquals(true, cache.isExists(seqNum3));
        assertEquals(false, cache.isExists(seqNum4));
        assertEquals(3, cache.getSize());

        cache.clear();
        assertEquals(0, cache.getSize());

    }

}