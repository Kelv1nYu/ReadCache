package com.kelv1n;

import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class Float4VectorWriter extends ArrowWriter<Float> {
    private float maxValue = Float.MAX_VALUE;
    private float minValue = Float.MIN_VALUE;

    Float4VectorWriter(VectorSchemaRoot schemaRoot,
                       Float4Vector floatVector,
                       ArrowCache arrowCache){
        super(schemaRoot, floatVector, arrowCache);
        floatVector.allocateNew();
    }

    @Override
    void writeVector(Float value){
        if (value < minValue) {
            minValue = value;
        }
        if (value > maxValue) {
            maxValue = value;
        }
        ((Float4Vector) valueVector).setSafe(currentCountOfEvents, value);
        valueVector.setValueCount(++currentCountOfEvents);

    }
}
