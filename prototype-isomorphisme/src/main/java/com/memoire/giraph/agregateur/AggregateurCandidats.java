package com.memoire.giraph.agregateur;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.Text;


public class AggregateurCandidats extends BasicAggregator<Text> {

    public static final String NOM = "agregateur.candidats";

    @Override
    public void aggregate(Text valeur) {
        getAggregatedValue().set(valeur.toString());
    }

    @Override
    public Text createInitialValue() {
        return new Text("");
    }

    /**
     * Encode les candidats et l'ordre dans la chaîne de cet agrégateur.
     *
     * @param idRacine  identifiant du sommet racine dans le motif
     * @param ordre     liste ordonnée des sommets du motif (DFS)
     * @param candidats map : idSommetMotif → liste d'identifiants candidats
     */
    public static Text encoder(
            long idRacine,
            java.util.List<Long> ordre,
            java.util.Map<Long, java.util.List<Long>> candidats) {

        StringBuilder sb = new StringBuilder();

        // ── Racine
        sb.append("RACINE:").append(idRacine).append("\n");

        // ── Ordre DFS
        sb.append("ORDRE:");
        for (int i = 0; i < ordre.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(ordre.get(i));
        }
        sb.append("\n");

        // ── Candidats par sommet du motif
        sb.append("CAND:");
        boolean premierSommet = true;
        for (long u : ordre) {
            if (!premierSommet) sb.append(";");
            premierSommet = false;

            sb.append(u).append("->");
            java.util.List<Long> cands = candidats.getOrDefault(
                    u, java.util.Collections.emptyList());
            for (int j = 0; j < cands.size(); j++) {
                if (j > 0) sb.append(",");
                sb.append(cands.get(j));
            }
        }

        return new Text(sb.toString());
    }

    /**
     * Décode la chaîne d'un agrégateur en un objet {@link ContenuCandidats}.
     */
    public static ContenuCandidats decoder(Text valeur) {
        ContenuCandidats resultat = new ContenuCandidats();
        String[] lignes = valeur.toString().split("\n");

        for (String ligne : lignes) {
            if (ligne.startsWith("RACINE:")) {
                resultat.racine = Long.parseLong(ligne.substring(7).trim());

            } else if (ligne.startsWith("ORDRE:")) {
                String[] tokens = ligne.substring(6).split(",");
                for (String t : tokens) {
                    if (!t.trim().isEmpty()) {
                        resultat.ordre.add(Long.parseLong(t.trim()));
                    }
                }

            } else if (ligne.startsWith("CAND:")) {
                String partie = ligne.substring(5);
                if (!partie.isEmpty()) {
                    for (String entree : partie.split(";")) {
                        String[] kv = entree.split("->");
                        long u = Long.parseLong(kv[0]);
                        java.util.List<Long> cands = new java.util.ArrayList<>();
                        if (kv.length > 1 && !kv[1].isEmpty()) {
                            for (String c : kv[1].split(",")) {
                                if (!c.trim().isEmpty()) {
                                    cands.add(Long.parseLong(c.trim()));
                                }
                            }
                        }
                        resultat.candidats.put(u, cands);
                    }
                }
            }
        }
        return resultat;
    }

    /** Conteneur DTO pour les données décodées de cet agrégateur. */
    public static class ContenuCandidats {
        public long racine = -1L;
        public java.util.List<Long> ordre
            = new java.util.ArrayList<>();
        public java.util.Map<Long, java.util.List<Long>> candidats
            = new java.util.LinkedHashMap<>();

        public boolean estValide() {
            return racine >= 0 && !ordre.isEmpty()
                && candidats.containsKey(racine)
                && !candidats.get(racine).isEmpty();
        }
    }
}
