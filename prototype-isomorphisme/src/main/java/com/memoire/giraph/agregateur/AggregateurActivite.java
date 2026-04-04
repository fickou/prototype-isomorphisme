package com.memoire.giraph.agregateur;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.LongWritable;

// Agrégateur d'activité par superstep.

public class AggregateurActivite extends BasicAggregator<LongWritable> {

    public static final String NOM = "agregateur.activite";

    @Override
    public void aggregate(LongWritable valeur) {
        getAggregatedValue().set(getAggregatedValue().get() + valeur.get());
    }

    @Override
    public LongWritable createInitialValue() {
        return new LongWritable(0L);
    }
}
