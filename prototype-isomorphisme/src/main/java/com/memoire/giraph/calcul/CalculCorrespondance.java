package com.memoire.giraph.calcul;

import com.memoire.giraph.agregateur.AggregateurActivite;
import com.memoire.giraph.agregateur.AggregateurCandidats;
import com.memoire.giraph.agregateur.AggregateurCandidats.ContenuCandidats;
import com.memoire.giraph.agregateur.AggregateurResultats;
import com.memoire.giraph.types.Message;
import com.memoire.giraph.types.Motif;
import com.memoire.giraph.types.ValeurSommet;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Phase 2 — Correspondance distribuée (adaptation d'Ullmann au modèle BSP).
 *
 * <p>Cette computation est exécutée à partir de la superstep 2.
 *
 * <h3>Superstep 2 (initialisation)</h3>
 * Chaque sommet v du graphe de données lit l'agrégateur
 * {@link AggregateurCandidats} pour obtenir M[] et l'ordre DFS.
 * Les candidats de la racine {@code u_0} lancent l'exploration en
 * envoyant un message EXPLORATION à leurs voisins (entrants et sortants).
 *
 * <h3>Superstep k > 2 (extension du mapping)</h3>
 * Chaque sommet v reçoit des messages EXPLORATION portant des mappings
 * partiels de profondeur {@code k−2}. Il tente de s'ajouter au mapping
 * en tant que candidat pour le prochain sommet du motif {@code u_{profondeur}}.
 * La vérification comprend :
 * <ul>
 *   <li>Injectivité (v pas déjà utilisé dans le mapping)</li>
 *   <li>Appartenance à {@code M[u_{profondeur}]}</li>
 *   <li>Cohérence structurelle de tous les arcs impliquant u_{profondeur}</li>
 * </ul>
 * Si le mapping est complet, il est agrégé dans {@link AggregateurResultats}.
 * Sinon, le mapping étendu est propagé aux voisins de v.
 *
 * - [x] Modifier `CalculFiltre.java` : maintenir les workers actifs après S1.
 * - [x] Modifier `MaitreUnique.java` : décaler le lancement de `CalculCorrespondance` à S3 et ajuster la surveillance.
 * - [x] Modifier `CalculCorrespondance.java` : adapter l'init à S3 et ajouter des logs détaillés.
 * - [x] Demander à l'utilisateur de compiler et tester.
 */
public class CalculCorrespondance
        extends BasicComputation<LongWritable, ValeurSommet, NullWritable, Message> {

    private static final Logger LOG = Logger.getLogger(CalculCorrespondance.class);

    /** Données des candidats lues depuis l'agrégateur (initialisées une fois par superstep). */
    private ContenuCandidats contenuCandidats;

    /** Référence au motif chargé depuis HDFS. */
    private Motif motif;

    @Override
    public void preSuperstep() {
        // Lire les candidats depuis l'agrégateur
        Text valeur = getAggregatedValue(AggregateurCandidats.NOM);
        contenuCandidats = AggregateurCandidats.decoder(valeur);

        if (getSuperstep() >= 2) {
            LOG.info("Superstep " + getSuperstep() + " raw aggregator value: [" + valeur.toString() + "]");
        }

        if (getSuperstep() >= 3 && !contenuCandidats.estValide()) {
            LOG.warn("Superstep " + getSuperstep() + " : Candidats non reçus ou invalides. " +
                     "Racine: " + contenuCandidats.racine + ", Ordre: " + contenuCandidats.ordre +
                     ", Candidats: " + contenuCandidats.candidats);
        }

        // Charger le motif depuis HDFS (singleton, chargé une seule fois par JVM)
        try {
            motif = Motif.charger(getConf());
        } catch (IOException e) {
            throw new RuntimeException("Impossible de charger le motif depuis HDFS", e);
        }
    }

    @Override
    public void compute(
            Vertex<LongWritable, ValeurSommet, NullWritable> sommet,
            Iterable<Message> messages) throws IOException {

        long superstep = getSuperstep();

        // ── Superstep 2 : Latence nécessaire pour la propagation des agrégateurs ──
        if (superstep == 2) {
            // On ne fait rien pour laisser l'agrégateur M[] arriver aux Workers à S3.
            // On ne vote PAS l'arrêt pour forcer le passage à S3.
            return;
        }

        if (!contenuCandidats.estValide()) {
            sommet.voteToHalt();
            return;
        }

        long idSommet    = sommet.getId().get();
        ValeurSommet val = sommet.getValue();
        Set<Long> voisinsSortantsSet = new HashSet<>();
        sommet.getEdges().forEach(e -> voisinsSortantsSet.add(e.getTargetVertexId().get()));

        if (superstep == 3) {
            // ── Superstep 3 : Initialisation (Candidats de la racine) ─────────────
            long racine = contenuCandidats.racine;
            List<Long> cands = contenuCandidats.candidats.getOrDefault(
                                    racine, Collections.emptyList());

            if (cands.contains(idSommet)) {
                Map<Long, Long> mappingInitial = new LinkedHashMap<>();
                mappingInitial.put(racine, idSommet);

                List<Long> ordre = contenuCandidats.ordre;
                if (ordre.size() == 1) {
                    LOG.info("Sommet " + idSommet + " : Isomorphisme complet trouvé (S3).");
                    aggregateMapping(mappingInitial);
                    aggregate(AggregateurActivite.NOM, new LongWritable(1L));
                } else {
                    LOG.info("Sommet " + idSommet + " : Racine trouvée, démarrage exploration.");
                    propaguerMapping(sommet, mappingInitial, voisinsSortantsSet, val);
                    aggregate(AggregateurActivite.NOM, new LongWritable(1L));
                }
            }
            // sommet.voteToHalt(); // Garde le sommet actif pour le suivi par le Master

        } else {
            // ── Superstep k > 3 : Extension des mappings reçus ────────────────────
            for (Message msg : messages) {
                if (msg.getMappingEncode() == null || msg.getMappingEncode().isEmpty()) continue;

                Map<Long, Long> mapping = decoderMapping(msg.getMappingEncode());
                int profondeur = mapping.size();
                List<Long> ordre = contenuCandidats.ordre;

                if (profondeur < ordre.size()) {
                    long prochainSommetMotif = ordre.get(profondeur);
                    List<Long> cands = contenuCandidats.candidats.getOrDefault(
                                           prochainSommetMotif, Collections.emptyList());

                    // Si ce sommet est un candidat valide pour la prochaine étape du motif
                    if (cands.contains(idSommet)) {
                        boolean coherent = motif.verifierCoherence(
                            mapping, prochainSommetMotif, idSommet,
                            voisinsSortantsSet, val.getVoisinsEntrants()
                        );

                        if (coherent) {
                            Map<Long, Long> mappingEtendu = new LinkedHashMap<>(mapping);
                            mappingEtendu.put(prochainSommetMotif, idSommet);

                            if (mappingEtendu.size() == ordre.size()) {
                                aggregateMapping(mappingEtendu);
                                aggregate(AggregateurActivite.NOM, new LongWritable(1L));
                                LOG.info("Sommet " + idSommet + " : Isomorphisme trouvé ! Mapping: " + mappingEtendu);
                            } else {
                                LOG.info("Sommet " + idSommet + " : Mapping étendu (profondeur " + mappingEtendu.size() + ")");
                                propaguerMapping(sommet, mappingEtendu, voisinsSortantsSet, val);
                                aggregate(AggregateurActivite.NOM, new LongWritable(1L));
                            }
                        }
                    }
                }
            }
            // sommet.voteToHalt(); // Laisser actif pour que le Master contrôle l'arrêt
        }
    }

    // ── Méthodes auxiliaires ──────────────────────────────────────────────────

    /**
     * Propage le mapping partiel à tous les voisins du sommet courant
     * (entrants + sortants).
     *
     * <p>Important : cette stratégie est correcte uniquement si l'ordre
     * d'exploration fourni par le master est linéairement explorable, c'est-à-dire
     * si chaque nouveau sommet du motif est adjacent au sommet du motif mappé à
     * l'étape précédente. Cette contrainte est validée côté master.
     */
    private void propaguerMapping(
            Vertex<LongWritable, ValeurSommet, NullWritable> sommet,
            Map<Long, Long> mapping,
            Set<Long> voisinsSortantsSet,
            ValeurSommet val) throws IOException {

        String mappingEncode = encoderMapping(mapping);
        Message msgExploration = Message.creerExploration(
            sommet.getId().get(), mappingEncode);

        Set<Long> destinataires = new LinkedHashSet<>();
        destinataires.addAll(voisinsSortantsSet);
        destinataires.addAll(val.getVoisinsEntrants());

        for (long idVoisin : destinataires) {
            sendMessage(new LongWritable(idVoisin), msgExploration);
        }
    }

    /** Agrège un isomorphisme complet dans {@link AggregateurResultats}. */
    private void aggregateMapping(Map<Long, Long> mapping) {
        aggregate(AggregateurResultats.NOM,
                  AggregateurResultats.encoderMapping(mapping));
    }

    /** Encode un mapping Map<Long,Long> → "u0:v0,u1:v1,..." */
    private static String encoderMapping(Map<Long, Long> mapping) {
        StringBuilder sb = new StringBuilder();
        boolean premier = true;
        for (Map.Entry<Long, Long> e : mapping.entrySet()) {
            if (!premier) sb.append(",");
            sb.append(e.getKey()).append(":").append(e.getValue());
            premier = false;
        }
        return sb.toString();
    }

    /** Décode "u0:v0,u1:v1,..." → Map<Long,Long> */
    private static Map<Long, Long> decoderMapping(String encode) {
        Map<Long, Long> mapping = new LinkedHashMap<>();
        if (encode == null || encode.isEmpty()) return mapping;
        for (String paire : encode.split(",")) {
            String[] kv = paire.split(":");
            if (kv.length == 2) {
                mapping.put(Long.parseLong(kv[0].trim()),
                            Long.parseLong(kv[1].trim()));
            }
        }
        return mapping;
    }
}
