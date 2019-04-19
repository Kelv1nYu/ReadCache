package com.kelv1n;


import io.netty.buffer.ByteBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import lombok.Getter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ArrowCacheHandle {
    LfuCache<Long, ByteBuffer> lfuCache;
    LruCache<Long, ByteBuffer> lruCache;
    FIFOCache<Long,ByteBuffer> fifoCache;
    RandomCache<Long, ByteBuffer> randomCache;
    ReplaceType cacheType;
    BufferAllocator readerAllocator;
    List<Field> readFields = new ArrayList<Field>();
    List<FieldVector> readFieldVectors = new ArrayList<FieldVector>();
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    IntVector intVector = new IntVector("test-vector", allocator);
    BigIntVector longVector = new BigIntVector("test-vector", allocator);
    ReadHandle readHandle;

    ByteBuffer cacheData;
    private String fieldName = "test-vector";


    public ArrowCacheHandle(ReplaceType replaceType, int capacity){
        this.cacheType = replaceType;
        switch (replaceType){
            case LFU:
                this.lfuCache = new LfuCache<Long, ByteBuffer>(capacity);
                break;
            case LRU:
                this.lruCache = new LruCache<Long, ByteBuffer>(capacity);
                break;
            case FIFO:
                this.fifoCache = new FIFOCache<Long, ByteBuffer>(capacity);
                break;
            case RANDOM:
                randomCache = new RandomCache<Long, ByteBuffer>(capacity);
                break;
        }
    }

    public CompletableFuture<ValueVector> getVector(List<Long> seqList){
        CompletableFuture<ValueVector> future = FutureUtils.createFuture();
        switch (cacheType){
            case LFU:
                lfuGetVector(seqList,future);
                break;
            case LRU:
                lruGetVector(seqList, future);
                break;
            case FIFO:
                fifoGetVector(seqList, future);
                break;
            case RANDOM:
                randomGetVector(seqList, future);
                break;
        }
        return future;
    }

    private void lfuGetVector(List<Long> seqList, CompletableFuture<ValueVector> future){
        Long seqNum;
        int listSize = seqList.size();
        if(listSize == 0){
            throw new NullPointerException("SeqList is empty!");
        }else if(listSize == 1){
            seqNum = seqList.get(0);
            if(lfuCache.isExists(seqNum)){
                cacheData = lfuCache.get(seqNum);
                String vectorType = getVectorName(cacheData);
                ByteBuffer readableData = sliceVector(cacheData);
                Data2Vector(vectorType, readableData, future);
            }else {
                readFromBk();
            }

        }else{
            for(int i = 0; i < seqList.size(); i++){
                seqNum = seqList.get(i);
                if(lfuCache.isExists(seqNum)){
                    cacheData = lfuCache.get(seqNum);
                    String vectorType = getVectorName(cacheData);
                    ByteBuffer readableData = sliceVector(cacheData);
                    Data2Vector(vectorType, readableData, future);
                }else {
                    readFromBk();
                }
            }
        }

    }
    private void lruGetVector(List<Long> seqList, CompletableFuture<ValueVector> future){
        Long seqNum;
        int listSize = seqList.size();
        if(listSize == 0){
            throw new NullPointerException("SeqList is empty!");
            //System.out.println("error : empty list!");
        }else if(listSize == 1){
            seqNum = seqList.get(0);
            if(lruCache.isExists(seqNum)){
                cacheData = lruCache.get(seqNum);
                String vectorType = getVectorName(cacheData);
                ByteBuffer readableData = sliceVector(cacheData);
                Data2Vector(vectorType, readableData, future);
            }else {
                readFromBk(readHandle, seqNum);
            }

        }else{
            for(int i = 0; i < seqList.size(); i++){
                seqNum = seqList.get(i);
                if(lruCache.isExists(seqNum)){
                    cacheData = lruCache.get(seqNum);
                    String vectorType = getVectorName(cacheData);
                    ByteBuffer readableData = sliceVector(cacheData);
                    Data2Vector(vectorType, readableData, future);
                }else {
                    readFromBk(readHandle, seqNum);
                }
            }
        }
    }
    private void fifoGetVector(List<Long> seqList, CompletableFuture<ValueVector> future){
        Long seqNum;
        int listSize = seqList.size();
        if(listSize == 0){
            throw new NullPointerException("SeqList is empty!");
        }else if(listSize == 1){
            seqNum = seqList.get(0);
            if(fifoCache.isExists(seqNum)){
                cacheData = fifoCache.get(seqNum);
                String vectorType = getVectorName(cacheData);
                ByteBuffer readableData = sliceVector(cacheData);
                Data2Vector(vectorType, readableData, future);
            }else {
                readFromBk();
            }

        }else{
            for(int i = 0; i < seqList.size(); i++){
                seqNum = seqList.get(i);
                if(fifoCache.isExists(seqNum)){
                    cacheData = fifoCache.get(seqNum);
                    String vectorType = getVectorName(cacheData);
                    ByteBuffer readableData = sliceVector(cacheData);
                    Data2Vector(vectorType, readableData, future);
                }else {
                    readFromBk();
                }
            }
        }
    }
    private void randomGetVector(List<Long> seqList, CompletableFuture<ValueVector> future){
        Long seqNum;
        int listSize = seqList.size();
        if(listSize == 0){
            throw new NullPointerException("SeqList is empty!");
        }else if(listSize == 1){
            seqNum = seqList.get(0);
            if(randomCache.isExists(seqNum)){
                cacheData = randomCache.get(seqNum);
                String vectorType = getVectorName(cacheData);
                ByteBuffer readableData = sliceVector(cacheData);
                Data2Vector(vectorType, readableData, future);
            }else {
                readFromBk();
            }

        }else{
            for(int i = 0; i < seqList.size(); i++){
                seqNum = seqList.get(i);
                if(randomCache.isExists(seqNum)){
                    cacheData = randomCache.get(seqNum);
                    String vectorType = getVectorName(cacheData);
                    ByteBuffer readableData = sliceVector(cacheData);
                    Data2Vector(vectorType, readableData, future);
                }else {
                    readFromBk();
                }
            }
        }
    }
    private void readFromBk(ReadHandle readHandle, long seqNum){


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

    public void testType(){
        Long l1 = 1L;
        Long l2 = 2L;
        Long l3 = 3L;
        Long l4 = 4L;
        Long l5 = 5L;
        ByteBuffer data = ByteBuffer.allocate(20);
        data.putInt(1);
        System.out.println(cacheType);
        switch (cacheType){
            case LFU:
                System.out.println(lfuCache.getClassName());
                break;
            case LRU:
                System.out.println(lruCache.getClassName());
                lruCache.put(l1, data);
                lruCache.put(l2, data);
                lruCache.put(l3, data);
                ByteBuffer v = lruCache.get(l1);
                lruCache.put(l4, data);
                //lruCache.put(l5, data);
                System.out.println(lruCache.getSize());
                System.out.println(lruCache.isExists(l1));
                System.out.println(lruCache.isExists(l2));
                System.out.println(lruCache.isExists(l3));
                System.out.println(lruCache.isExists(l4));
                //System.out.println(lruCache.isExists(l5));
                break;
            case FIFO:
                System.out.println(fifoCache.getClassName());
                break;
            case RANDOM:
                System.out.println(randomCache.getClassName());
                randomCache.put(l1, data);
                randomCache.put(l2, data);
                randomCache.put(l3, data);
                ByteBuffer v2 = randomCache.get(l1);
                randomCache.put(l4, data);
                //lruCache.put(l5, data);
                System.out.println(randomCache.getSize());
                System.out.println(randomCache.isExists(l1));
                System.out.println(randomCache.isExists(l2));
                System.out.println(randomCache.isExists(l3));
                System.out.println(randomCache.isExists(l4));
                //System.out.println(lruCache.isExists(l5));
                break;
        }

    }

    public void testWrite(){

        List<Field> fields = new ArrayList<Field>();
        List<FieldVector> fieldVectors = new ArrayList<FieldVector>();


        fields.add(intVector.getField());
        fieldVectors.add(intVector);
        VectorSchemaRoot schemaRoot = new VectorSchemaRoot(fields, fieldVectors, 0);
        IntVectorWriter intVectorWriter = new IntVectorWriter(schemaRoot, intVector, lruCache);

        for (int i = 0; i < 10; i++) {
             intVectorWriter.writeArrow(i, 0);
        }

        intVectorWriter.writeArrow(-1, 1);

        System.out.println(lruCache.getSize());
        //ByteBuffer readableData = lruCache.get(1L);
        //System.out.println(readableData);
        //readableData.get(0);
        //int size = readableData.getInt();



        /*IntVector tmpVector = (IntVector) columnGroupSchema.getVector(fieldName);
        for (int i = 0; i < tmpVector.getValueCount(); i++) {
            intVector.setSafe(rowId++, tmpVector.get(i));
        }
        tmpVector.close();*/


    }


    public void testRead(){

        List<Field> fields = new ArrayList<Field>();
        List<FieldVector> fieldVectors = new ArrayList<FieldVector>();


        fields.add(intVector.getField());
        fieldVectors.add(intVector);
        VectorSchemaRoot schemaRoot = new VectorSchemaRoot(fields, fieldVectors, 0);
        IntVectorWriter intVectorWriter = new IntVectorWriter(schemaRoot, intVector, lruCache);

        for (int i = 0; i < 10; i++) {
            intVectorWriter.writeArrow(i, 0);
        }

        intVectorWriter.writeArrow(-1, 1);
        //System.out.println(intVectorWriter.getCurrentCountOfEvents());
        intVectorWriter.storeInCache(intVectorWriter.getCurrentCountOfEvents(), 1);



        /*long seqNum1 = 0;
        long seqNum2 = 1;
        long seqNum3 = 2;
        long seqNum4 = 3;



        System.out.println(lruCache.getSize());
        System.out.println(lruCache.isExists(seqNum1));
        System.out.println(lruCache.isExists(seqNum2));*/


        /*BufferAllocator readerAllocator = new RootAllocator(Long.MAX_VALUE);
        // we should construct one seperate schemaroot for read column group data to avoid conflict with writer
        IntVector vectorForReadColumnGroup = new IntVector(fieldName, readerAllocator);
        List<Field> readerFields = new ArrayList<>();
        List<FieldVector> readerFieldVectors = new ArrayList<>();
        readerFields.add(vectorForReadColumnGroup.getField());
        readerFieldVectors.add(vectorForReadColumnGroup);
        VectorSchemaRoot readSchemaRoot = new VectorSchemaRoot(readerFields, readerFieldVectors, 0);
        IntVectorReader intVectorReader = new IntVectorReader(readSchemaRoot, fieldName, readerAllocator);

        ByteBuffer readableData = lruCache.get(0L);

        intVectorReader.read(readableData);*/

        fields.clear();
        fields.add(longVector.getField());
        fieldVectors.clear();
        fieldVectors.add(longVector);
        VectorSchemaRoot schemaRoot2 = new VectorSchemaRoot(fields, fieldVectors, 0);
        BigIntVectorWriter bigIntVectorWriter= new BigIntVectorWriter(schemaRoot2, longVector, lruCache);

        for (int i = 0; i < 10; i++) {
            bigIntVectorWriter.writeArrow(1L, 2);
        }



        bigIntVectorWriter.writeArrow(-1L, 3);
        /*System.out.println(lruCache.getSize());
        System.out.println(lruCache.isExists(seqNum1));
        System.out.println(lruCache.isExists(seqNum2));
        System.out.println(lruCache.isExists(seqNum3));*/

        bigIntVectorWriter.storeInCache(bigIntVectorWriter.getCurrentCountOfEvents(), 3);



        /*System.out.println(lruCache.getSize());
        System.out.println(lruCache.isExists(seqNum1));
        System.out.println(lruCache.isExists(seqNum2));
        System.out.println(lruCache.isExists(seqNum3));
        System.out.println(lruCache.isExists(seqNum4));*/
        //ByteBuffer readableData = lruCache.get(1L);
        //System.out.println(readableData);
        List<Long> testSeqNum = new ArrayList<>();
        testSeqNum.add(1L);
        //getVector(testSeqNum);

        //testSeqNum.clear();
        //getVector(testSeqNum);
        testSeqNum.add(2L);
        testSeqNum.add(3L);
        getVector(testSeqNum);





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
