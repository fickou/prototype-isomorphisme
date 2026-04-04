package com.memoire.giraph.agregateur;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.Text;


public class AggregateurDegresSommets extends BasicAggregator<Text> {

    public static final String NOM = "agregateur.degres.sommets";

    public static final String SEP_SOMMET = "|";

    /**
     * Encode une triplet (id, degréEntrant, degreSortant) pour cet agrégateur.
     *
     * @param id          identifiant du sommet
     * @param degreEntrant  degré entrant du sommet
     * @param degreSortant  degré sortant du sommet
     */
    public static Text encoder(long id, int degreEntrant, int degreSortant) {
        return new Text(id + ":" + degreEntrant + ":" + degreSortant);
    }

    @Override
    public void aggregate(Text valeur) {
        Text actuelle = getAggregatedValue();
        String s = actuelle.toString();
        if (s.isEmpty()) {
            actuelle.set(valeur.toString());
        } else {
            actuelle.set(s + SEP_SOMMET + valeur.toString());
        }
    }

    @Override
    public Text createInitialValue() {
        return new Text("");
    }
}
