package com.kelv1n;

import org.apache.arrow.memory.BufferAllocator;

import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class Float8VectorReader extends ArrowReader<Double> {
    private String fieldName;
    private Float8Vector currentVector;


    Float8VectorReader(VectorSchemaRoot schemaRoot,
                       String fieldName,
                       BufferAllocator bufferAllocator){
        super(bufferAllocator, schemaRoot);
        this.fieldName = fieldName;
    }

    @Override
    public void read(ByteBuffer readableData, CompletableFuture<ValueVector> future){
        loadData2Vector(readableData);
        currentVector = (Float8Vector) schemaRoot.getVector(fieldName);
        future.complete(currentVector);
        /*int a = currentVector.getValueCount();
        System.out.println(a);

        for(int i = 0; i < a; i++){
            System.out.println(currentVector.get(i));
        }*/
    }
}
