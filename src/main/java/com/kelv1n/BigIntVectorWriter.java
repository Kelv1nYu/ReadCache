package com.kelv1n;


import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class BigIntVectorWriter extends ArrowWriter<Long>{

    private long maxValue = Long.MAX_VALUE;
    private long minValue = Long.MIN_VALUE;

    BigIntVectorWriter(VectorSchemaRoot schemaRoot,
                       BigIntVector longVector,
                       ArrowCache arrowCache){
        super(schemaRoot, longVector, arrowCache);
        longVector.allocateNew();
    }

    @Override
    void writeVector(Long value){
        if (value < minValue) {
            minValue = value;
        }
        if (value > maxValue) {
            maxValue = value;
        }
        ((BigIntVector) valueVector).setSafe(currentCountOfEvents, value);
        valueVector.setValueCount(++currentCountOfEvents);

    }

}
