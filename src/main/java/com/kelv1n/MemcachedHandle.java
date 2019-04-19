package com.kelv1n;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.bookkeeper.common.concurrent.FutureUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MemcachedHandle {
    MemcachedCache cache = new MemcachedCache();
    ByteBuffer cacheData;
    BufferAllocator readerAllocator;
    List<Field> readFields = new ArrayList<Field>();
    List<FieldVector> readFieldVectors = new ArrayList<FieldVector>();
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    private String fieldName = "test-vector";

    MemcachedHandle(){

    }

    public CompletableFuture<ValueVector> getVector(List<Long> list){
        CompletableFuture<ValueVector> future = FutureUtils.createFuture();
        long seqNum;
        int listSize = list.size();
        if(listSize == 0){
            throw new NullPointerException("SeqList is empty!");
        }else if(listSize == 1){
            seqNum = list.get(0);
            if(cache.isExists(seqNum)){
                cacheData = (ByteBuffer) cache.get(seqNum);
                String vectorType = getVectorName(cacheData);
                ByteBuffer readableData = sliceVector(cacheData);
                Data2Vector(vectorType, readableData, future);
            }

        }else{
            for(int i = 0; i < list.size(); i++){
                seqNum = list.get(i);
                if(cache.isExists(seqNum)){
                    cacheData = (ByteBuffer) cache.get(seqNum);
                    String vectorType = getVectorName(cacheData);
                    ByteBuffer readableData = sliceVector(cacheData);
                    Data2Vector(vectorType, readableData, future);
                }else {
                    readFromBk();
                }
            }

        }
        return future;
    }

    private void readFromBk(){

    }

    private String getVectorName(ByteBuffer data){
        int length = data.remaining();
        int size = data.getInt();
        data.position(4);
        data.limit(4 + size);
        ByteBuffer vectorInfo = data.slice();
        String vectorName = ArrowSerializer.deserializerMinorType(vectorInfo);
        data.position(0);
        data.limit(length);
        //System.out.println(data.limit());
        return vectorName;
    }

    private ByteBuffer sliceVector(ByteBuffer data){
        int size = data.getInt();
        data.position(data.position() + size);
        return data;
    }

    private void Data2Vector(String vectorType, ByteBuffer readableData, CompletableFuture<ValueVector> future){
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

    private void readIntVector(ByteBuffer data, CompletableFuture<ValueVector> future){
        readerAllocator = new RootAllocator(Long.MAX_VALUE);
        readFields.clear();
        readFieldVectors.clear();
        IntVector intVectorForRead = new IntVector(fieldName, readerAllocator);
        readFields.add(intVectorForRead.getField());
        readFieldVectors.add(intVectorForRead);
        VectorSchemaRoot readSchemaRoot = new VectorSchemaRoot(readFields, readFieldVectors, 0);
        IntVectorReader intVectorReader = new IntVectorReader(readSchemaRoot, fieldName, readerAllocator);
        intVectorReader.read(data, future);
    }

    private void readBigIntVector(ByteBuffer data, CompletableFuture<ValueVector> future){
        readerAllocator = new RootAllocator(Long.MAX_VALUE);
        readFields.clear();
        readFieldVectors.clear();
        BigIntVector longVectorForRead = new BigIntVector(fieldName, readerAllocator);
        readFields.add(longVectorForRead.getField());
        readFieldVectors.add(longVectorForRead);
        VectorSchemaRoot readSchemaRoot = new VectorSchemaRoot(readFields, readFieldVectors, 0);
        BigIntVectorReader longVectorReader = new BigIntVectorReader(readSchemaRoot, fieldName, readerAllocator);
        longVectorReader.read(data, future);

    }

    private void readFloat4Vector(ByteBuffer data, CompletableFuture<ValueVector> future){
        readerAllocator = new RootAllocator(Long.MAX_VALUE);
        readFields.clear();
        readFieldVectors.clear();
        Float4Vector float4VectorForRead = new Float4Vector(fieldName, readerAllocator);
        readFields.add(float4VectorForRead.getField());
        readFieldVectors.add(float4VectorForRead);
        VectorSchemaRoot readSchemaRoot = new VectorSchemaRoot(readFields, readFieldVectors, 0);
        Float4VectorReader float4VectorReader = new Float4VectorReader(readSchemaRoot, fieldName, readerAllocator);
        float4VectorReader.read(data, future);

    }

    private void readFloat8Vector(ByteBuffer data, CompletableFuture<ValueVector> future){
        readerAllocator = new RootAllocator(Long.MAX_VALUE);
        readFields.clear();
        readFieldVectors.clear();
        Float8Vector float8VectorForRead = new Float8Vector(fieldName, readerAllocator);
        readFields.add(float8VectorForRead.getField());
        readFieldVectors.add(float8VectorForRead);
        VectorSchemaRoot readSchemaRoot = new VectorSchemaRoot(readFields, readFieldVectors, 0);
        Float4VectorReader float4VectorReader = new Float4VectorReader(readSchemaRoot, fieldName, readerAllocator);
        float4VectorReader.read(data, future);

    }
}
