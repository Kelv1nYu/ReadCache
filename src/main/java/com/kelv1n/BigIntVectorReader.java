package com.kelv1n;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class BigIntVectorReader extends ArrowReader<Long>{

    private String fieldName;
    private BigIntVector currentVector;


    BigIntVectorReader(VectorSchemaRoot schemaRoot,
                    String fieldName,
                    BufferAllocator bufferAllocator){
        super(bufferAllocator, schemaRoot);
        this.fieldName = fieldName;
    }

    @Override
    public void read(ByteBuffer readableData, CompletableFuture<ValueVector> future){
        loadData2Vector(readableData);
        currentVector = (BigIntVector) schemaRoot.getVector(fieldName);
        future.complete(currentVector);
        /*int a = currentVector.getValueCount();
        System.out.println("currentCountOfEvents of BigIntVector" + a);

        for(int i = 0; i < a; i++){
            System.out.println(currentVector.get(i));
        }*/


    }
}
