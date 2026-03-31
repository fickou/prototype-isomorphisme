package com.memoire.giraph.agregateur;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.LongWritable;

/**
 * Agrégateur d'activité par superstep.
 *
 * <p>Chaque worker incrémente cet agrégateur lorsqu'il :
 * <ul>
 *   <li>propage un mapping partiel, ou</li>
 *   <li>trouve un isomorphisme complet.</li>
 * </ul>
 *
 * <p>Le master lit la valeur agrégée à la superstep suivante. Si cette valeur
 * vaut 0, cela signifie qu'aucun nouveau travail utile n'a été produit pendant
 * la superstep précédente ; le master peut alors écrire les résultats et arrêter
 * explicitement la computation.
 */
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
