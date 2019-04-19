package com.kelv1n;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class IntVectorWriter extends ArrowWriter<Integer> {

    private int maxValue = Integer.MAX_VALUE;
    private int minValue = Integer.MIN_VALUE;

    IntVectorWriter(VectorSchemaRoot schemaRoot,
                    IntVector intVector,
                    ArrowCache arrowCache){
        super(schemaRoot, intVector, arrowCache);
        intVector.allocateNew();
    }

    @Override
    void writeVector(Integer value){
        if (value < minValue) {
            minValue = value;
        }
        if (value > maxValue) {
            maxValue = value;
        }
        ((IntVector) valueVector).setSafe(currentCountOfEvents, value);
        valueVector.setValueCount(++currentCountOfEvents);

    }
}
