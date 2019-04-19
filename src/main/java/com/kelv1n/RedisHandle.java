package com.kelv1n;


import org.apache.arrow.flatbuf.Null;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.bookkeeper.common.concurrent.FutureUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class RedisHandle {

    private RedisCache cache = new RedisCache();
    ByteBuffer cacheData;
    BufferAllocator readerAllocator;
    List<Field> readFields = new ArrayList<Field>();
    List<FieldVector> readFieldVectors = new ArrayList<FieldVector>();
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    private String fieldName = "test-vector";


    public CompletableFuture<ValueVector> getVector(List<Long> list){
        CompletableFuture<ValueVector> future = FutureUtils.createFuture();
        long seqNum;
        int listSize = list.size();
        if(listSize == 0){
            throw new NullPointerException("SeqList is empty!");
        }else if(listSize == 1){
            seqNum = list.get(0);
            if(cache.isExists(seqNum)) {
                List<byte[]> data = (List<byte[]>) cache.get(seqNum);
                String vectorType = new String(data.get(0));
                Data2Vector(vectorType, data, future);
            }else {
                readFromBk();
            }
        }else{
            for(int i = 0; i < listSize; i++){
                seqNum = list.get(i);
                if(cache.isExists(seqNum)){
                    List<byte[]> data = (List<byte[]>) cache.get(seqNum);
                    String vectorType = new String(data.get(0));
                    Data2Vector(vectorType, data, future);
                }else {
                    readFromBk();
                }
            }

        }
        return future;
    }

    private void readFromBk(){

    }


    private void Data2Vector(String vectorType, List<byte[]> readableData, CompletableFuture<ValueVector> future){
        switch (vectorType){
            case "INT": readIntVector(readableData, future);
                break;
            case "BIGINT":readBigIntVector(readableData, future);
                break;
            case "FLOAT4":readFloat4Vector(readableData, future);
                break;
            case "FLOAT8":readFloat8Vector(readableData, future);
                break;
        }
    }

    private void readIntVector(List<byte[]> data, CompletableFuture<ValueVector> future){
        IntVector intVector = new IntVector("", allocator);
        intVector.setValueCount(data.size() - 1);
        for(int i = 1; i < data.size(); i++){
            String str = new String(data.get(i));
            int event = Integer.parseInt(str);
            intVector.setSafe(i - 1, event);
        }
        future.complete(intVector);
    }

    private void readBigIntVector(List<byte[]>  data, CompletableFuture<ValueVector> future){
        BigIntVector longVector = new BigIntVector("", allocator);
        longVector.setValueCount(data.size() - 1);
        for(int i = 1; i < data.size(); i++){
            String str = new String(data.get(i));
            long event = Long.parseLong(str);
            longVector.setSafe(i - 1, event);
        }

        future.complete(longVector);

    }

    private void readFloat4Vector(List<byte[]>  data, CompletableFuture<ValueVector> future){
        Float4Vector floatVector = new Float4Vector("", allocator);
        floatVector.setValueCount(data.size() - 1);
        for(int i = 1; i < data.size(); i++){
            String str = new String(data.get(i));
            float event = Float.parseFloat(str);
            floatVector.setSafe(i - 1, event);
        }
        future.complete(floatVector);

    }

    private void readFloat8Vector(List<byte[]>  data, CompletableFuture<ValueVector> future){
        Float8Vector doubleVector = new Float8Vector("", allocator);
        doubleVector.setValueCount(data.size() - 1);
        for(int i = 1; i < data.size(); i++){
            String str = new String(data.get(i));
            double event = Double.parseDouble(str);
            doubleVector.setSafe(i - 1, event);
        }

        future.complete(doubleVector);
    }
}

