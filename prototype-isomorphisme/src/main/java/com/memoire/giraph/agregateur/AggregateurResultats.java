package com.memoire.giraph.agregateur;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.Text;

/**
 * Agrégateur persistant qui accumule tous les isomorphismes complets
 * découverts au cours de la phase de correspondance.
 *
 * <p>Chaque isomorphisme trouvé est encodé comme un mapping complet
 * {@code "u0:v0,u1:v1,...,uk:vk"} et les mappings sont séparés par {@code "|"}.
 *
 * <p>L'agrégateur est persistant (ne se réinitialise pas entre les supersteps)
 * afin d'accumuler les résultats tout au long de l'exécution. Le master lit
 * la valeur finale et l'écrit dans un fichier HDFS unique.
 *
 * <p>Note sur la déduplication : dans ce prototype, des doublons sont
 * possibles si le même mapping est découvert via plusieurs chemins. Une
 * déduplication est appliquée lors de l'écriture par le master.
 */
public class AggregateurResultats extends BasicAggregator<Text> {

    public static final String NOM = "agregateur.resultats";

    /** Séparateur entre les isomorphismes. */
    public static final String SEP = "|";

    @Override
    public void aggregate(Text valeur) {
        if (valeur == null || valeur.toString().isEmpty()) {
            return;
        }
        Text actuelle = getAggregatedValue();
        String s = actuelle.toString();
        if (s.isEmpty()) {
            actuelle.set(valeur.toString());
        } else {
            // Concaténation : commutative et associative
            actuelle.set(s + SEP + valeur.toString());
        }
    }

    @Override
    public Text createInitialValue() {
        return new Text("");
    }

    /**
     * Encode un mapping complet (motif → données) pour cet agrégateur.
     *
     * @param mapping map idSommetMotif → idSommetDonnees
     * @return encodage "u0:v0,u1:v1,..."
     */
    public static Text encoderMapping(java.util.Map<Long, Long> mapping) {
        StringBuilder sb = new StringBuilder();
        boolean premier = true;
        for (java.util.Map.Entry<Long, Long> e : mapping.entrySet()) {
            if (!premier) sb.append(",");
            sb.append(e.getKey()).append(":").append(e.getValue());
            premier = false;
        }
        return new Text(sb.toString());
    }

    /**
     * Decode et retourne la liste de tous les isomorphismes uniques.
     * Applique une déduplication basée sur la représentation canonique
     * (tri des entrées par id de sommet motif).
     */
    public static java.util.List<java.util.Map<Long, Long>> decoder(Text valeur) {
        java.util.Set<String> vus = new java.util.LinkedHashSet<>();
        java.util.List<java.util.Map<Long, Long>> resultats = new java.util.ArrayList<>();

        String tout = valeur.toString();
        if (tout.isEmpty()) return resultats;

        for (String entree : tout.split("\\|")) {
            entree = entree.trim();
            if (entree.isEmpty()) continue;

            // Normalisation pour déduplication
            java.util.Map<Long, Long> mapping = new java.util.TreeMap<>();
            for (String paire : entree.split(",")) {
                String[] kv = paire.split(":");
                if (kv.length == 2) {
                    mapping.put(Long.parseLong(kv[0]), Long.parseLong(kv[1]));
                }
            }
            String cle = mapping.toString();
            if (vus.add(cle)) {   // add retourne false si déjà présent
                resultats.add(mapping);
            }
        }
        return resultats;
    }
}
