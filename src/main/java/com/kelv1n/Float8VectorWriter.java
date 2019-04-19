package com.kelv1n;

import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class Float8VectorWriter extends ArrowWriter<Double> {
    private double maxValue = Double.MAX_VALUE;
    private double minValue = Double.MIN_VALUE;

    Float8VectorWriter(VectorSchemaRoot schemaRoot,
                       Float8Vector doubleVector,
                       ArrowCache arrowCache){
        super(schemaRoot, doubleVector, arrowCache);
        doubleVector.allocateNew();
    }

    @Override
    void writeVector(Double value){
        if (value < minValue) {
            minValue = value;
        }
        if (value > maxValue) {
            maxValue = value;
        }
        ((Float8Vector) valueVector).setSafe(currentCountOfEvents, value);
        valueVector.setValueCount(++currentCountOfEvents);

    }
}
