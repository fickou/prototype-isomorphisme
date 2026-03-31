package com.memoire.giraph.agregateur;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.Text;

/**
 * Agrégateur des paires de degrés de tous les sommets du graphe de données.
 *
 * <p>Utilisé à la fin de la superstep 1 (phase filtre) pour que le master
 * puisse construire les ensembles de candidats M[u] pour chaque sommet u
 * du motif.
 *
 * <p>Format encodé : {@code "id0:indeg0:outdeg0|id1:indeg1:outdeg1|..."}
 * L'opération d'agrégation est une concaténation avec le séparateur {@code "|"},
 * ce qui est commutative et associative.
 */
public class AggregateurDegresSommets extends BasicAggregator<Text> {

    public static final String NOM = "agregateur.degres.sommets";

    /** Séparateur entre les entrées de sommets différents. */
    public static final String SEP_SOMMET = "|";

    /**
     * Encode une triplet (id, degréEntrant, degreSortant) pour cet agrégateur.
     *
     * @param id          identifiant du sommet
     * @param degreEntrant  degré entrant du sommet
     * @param degreSortant  degré sortant du sommet
     * @return chaîne encodée "id:degreEntrant:degreSortant"
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
