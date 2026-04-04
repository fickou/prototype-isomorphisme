package com.memoire.giraph.calcul;

import com.memoire.giraph.agregateur.AggregateurActivite;
import com.memoire.giraph.agregateur.AggregateurCandidats;
import com.memoire.giraph.agregateur.AggregateurDegresSommets;
import com.memoire.giraph.agregateur.AggregateurResultats;
import com.memoire.giraph.types.Motif;
import com.memoire.giraph.types.PaireDegres;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 Master unique gérant les deux phases du job en un seul job Giraph.
 Choix de la racine
 Le sommet du motif ayant le plus petit nombre de candidats dans le graphe
 de données est choisi comme racine (heuristique de réduction de l'espace
 de recherche, Ullmann).
 */
public class MaitreUnique extends DefaultMasterCompute {

    private static final Logger LOG = Logger.getLogger(MaitreUnique.class);

    /** Nombre max de supersteps de correspondance avant abandon. */
    private static final int MAX_SUPERSTEPS_CORRESPONDANCE = 1000;

    /** Pour éviter une double écriture des résultats. */
    private boolean resultatsEcrits = false;

    /** Sauvegarde des candidats à propager à chaque superstep. */
    private Text candidatsEncoded = null;

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        // Enregistrement des agrégateurs
        // Régulier : réinitialisé à chaque superstep
        registerAggregator(AggregateurDegresSommets.NOM,
                AggregateurDegresSommets.class);

        // Régulier : réinitialisé à chaque superstep (on le réécrit depuis le Master)
        registerAggregator(AggregateurCandidats.NOM,
                AggregateurCandidats.class);

        // Persistants : conservent leur valeur d'un superstep à l'autre
        registerPersistentAggregator(AggregateurResultats.NOM,
                AggregateurResultats.class);

        // Régulier : compteur d'activité pour détecter proprement la fin
        registerAggregator(AggregateurActivite.NOM,
                AggregateurActivite.class);

        // Phase 1 : démarrer avec CalculFiltre
        setComputation(CalculFiltre.class);
    }

    @Override
    public void compute() {
        long superstep = getSuperstep();
        LOG.info("MaitreUnique.compute() superstep=" + superstep);

        if (superstep == 0) {
            // CalculFiltre envoie les pings
            setComputation(CalculFiltre.class);

        } else if (superstep == 1) {
            // CalculFiltre collecte les degrés et agrège
            setComputation(CalculFiltre.class);

        } else if (superstep == 2) {
            construireEtDistribuerCandidats();

            setComputation(CalculCorrespondance.class);

        } else {
            // Surveillance de la convergence (superstep ≥ 3)
            if (candidatsEncoded != null) {
                setAggregatedValue(AggregateurCandidats.NOM, candidatsEncoded);
            }
            surveillerConvergence(superstep);
        }
    }
    // Phase de transition : construction des candidats M[]

    private void construireEtDistribuerCandidats() {
        // Lire les degrés agrégés par les workers en superstep 1
        Text degresText = getAggregatedValue(AggregateurDegresSommets.NOM);
        Map<Long, PaireDegres> degresGraphe = parserDegresSommets(degresText);

        // Charger le motif depuis HDFS
        Motif motif;
        try {
            motif = Motif.charger(getConf());
        } catch (IOException e) {
            LOG.error("Impossible de charger le motif : " + e.getMessage());
            haltComputation();
            return;
        }

        LOG.info("Motif chargé :\n" + motif);

        // Filtrage : construction des candidats M[u] pour chaque u du motif
        Map<Long, List<Long>> candidats = new LinkedHashMap<>();
        for (long u : motif.getSommets()) {
            PaireDegres degresMotif = motif.getDegres(u);
            List<Long> cands = new ArrayList<>();
            for (Map.Entry<Long, PaireDegres> e : degresGraphe.entrySet()) {
                if (e.getValue().estCandidatPour(degresMotif)) {
                    cands.add(e.getKey());
                }
            }
            candidats.put(u, cands);
            LOG.info("M[" + u + "] = " + cands);

            if (cands.isEmpty()) {
                haltComputation();
                return;
            }
        }

        // Choisir la racine
        long idRacine = choisirRacine(candidats, motif);
        LOG.info("Racine choisie : " + idRacine
                + " (" + candidats.get(idRacine).size() + " candidats)");

        // Calculer l'ordre d'exploration depuis la racine
        List<Long> ordreExploration = motif.calculerOrdreDFS(idRacine);
        LOG.info("Ordre d'exploration DFS : " + ordreExploration);

        if (!motif.ordreEstLineairementExplorable(ordreExploration)) {
            haltComputation();
            return;
        }

        // Distribuer aux workers via l'agrégateur persistant 
        candidatsEncoded = AggregateurCandidats.encoder(idRacine, ordreExploration, candidats);
        setAggregatedValue(AggregateurCandidats.NOM, candidatsEncoded);
        LOG.info("Candidats envoyés à l'agrégateur. Racine = " + idRacine);
    }

    // Choix de la racine
    private long choisirRacine(Map<Long, List<Long>> candidats, Motif motif) {
        long meilleureRacine = -1L;
        int minCandidats = Integer.MAX_VALUE;

        // 1) Chercher parmi les sommets sources (degré entrant = 0 dans le motif)
        for (Map.Entry<Long, List<Long>> e : candidats.entrySet()) {
            long u = e.getKey();
            if (motif.getVoisinsEntrants(u).isEmpty()) {
                if (e.getValue().size() < minCandidats) {
                    minCandidats = e.getValue().size();
                    meilleureRacine = u;
                }
            }
        }

        // 2) Fallback : si aucun sommet source, prendre celui avec le moins de candidats
        if (meilleureRacine == -1L) {
            for (Map.Entry<Long, List<Long>> e : candidats.entrySet()) {
                if (e.getValue().size() < minCandidats) {
                    minCandidats = e.getValue().size();
                    meilleureRacine = e.getKey();
                }
            }
        }

        return meilleureRacine;
    }

    // Surveillance de la convergence 

    private void surveillerConvergence(long superstep) {
        Text resultats = getAggregatedValue(AggregateurResultats.NOM);
        LongWritable activiteAgg = getAggregatedValue(AggregateurActivite.NOM);
        long activite = activiteAgg == null ? 0L : activiteAgg.get();

        if (superstep >= 4 && activite == 0L) {
            LOG.info("Aucune nouvelle activité détectée : écriture des résultats et arrêt propre.");
            if (!resultatsEcrits) {
                ecrireResultats(resultats);
                resultatsEcrits = true;
            }
            haltComputation();
            return;
        }

        long superstepCorrespondance = superstep - 3;
        if (superstepCorrespondance >= MAX_SUPERSTEPS_CORRESPONDANCE) {
            LOG.warn("Nombre maximum de supersteps atteint. Arrêt forcé.");
            if (!resultatsEcrits) {
                ecrireResultats(resultats);
                resultatsEcrits = true;
            }
            haltComputation();
        }
    }

    // Écriture des résultats 

    private void ecrireResultats(Text resultats) {
        String cheminSortie = getConf().get("resultats.chemin", "/resultats-isomorphismes.txt");
        List<Map<Long, Long>> liste = AggregateurResultats.decoder(resultats);

        try {
            FileSystem fs = FileSystem.get(getConf());
            Path path = new Path(cheminSortie);
            if (fs.exists(path)) {
                fs.delete(path, false);
            }
            try (FSDataOutputStream out = fs.create(path)) {
                out.writeBytes("# Isomorphismes trouvés : " + liste.size() + "\n");
                out.writeBytes("# Format : sommetMotif:sommetDonnees,...\n");
                for (int i = 0; i < liste.size(); i++) {
                    out.writeBytes("ISO_" + (i + 1) + " : ");
                    StringBuilder sb = new StringBuilder();
                    for (Map.Entry<Long, Long> e : liste.get(i).entrySet()) {
                        if (sb.length() > 0)
                            sb.append(", ");
                        sb.append(e.getKey()).append(" -> ").append(e.getValue());
                    }
                    out.writeBytes(sb.toString() + "\n");
                }
            }
        } catch (IOException e) {
            LOG.error("Erreur lors de l'écriture des résultats : " + e.getMessage());
        }
    }

    public void finaliserEtEcrire() {
        Text resultats = getAggregatedValue(AggregateurResultats.NOM);
        if (resultats != null) {
            ecrireResultats(resultats);
        }
    }

    // Parsing des degrés 

    private static Map<Long, PaireDegres> parserDegresSommets(Text degresText) {
        Map<Long, PaireDegres> resultat = new LinkedHashMap<>();
        if (degresText == null || degresText.toString().isEmpty()) {
            return resultat;
        }
        for (String entree : degresText.toString().split("\\|")) {
            entree = entree.trim();
            if (entree.isEmpty())
                continue;
            String[] parts = entree.split(":");
            if (parts.length == 3) {
                long id = Long.parseLong(parts[0]);
                int degreEntrant = Integer.parseInt(parts[1]);
                int degreSortant = Integer.parseInt(parts[2]);
                resultat.put(id, new PaireDegres(degreEntrant, degreSortant));
            }
        }
        return resultat;
    }
}
