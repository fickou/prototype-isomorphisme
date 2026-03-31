package com.memoire.giraph.calcul;

import com.memoire.giraph.agregateur.AggregateurDegresSommets;
import com.memoire.giraph.types.Message;
import com.memoire.giraph.types.ValeurSommet;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Phase 1 — Filtrage : calcul des degrés des sommets du graphe de données.
 *
 * <p>Cette computation est exécutée pendant les supersteps 0 et 1.
 *
 * <h3>Superstep 0</h3>
 * Chaque sommet v envoie un message {@link Message#PING_DEGRE} à tous ses
 * voisins sortants. Ce signal permettra à chaque voisin de compter son
 * degré entrant.
 *
 * <h3>Superstep 1</h3>
 * Chaque sommet v :
 * <ol>
 *   <li>Compte les messages reçus = son degré entrant.</li>
 *   <li>Mémorise l'identifiant des émetteurs = ses voisins entrants.</li>
 *   <li>Agrège sa paire de degrés (degreEntrant, degreSortant) dans
 *       {@link AggregateurDegresSommets}.</li>
 *   <li>Vote pour l'arrêt (la superstep suivante appartient au master).</li>
 * </ol>
 */
public class CalculFiltre
        extends BasicComputation<LongWritable, ValeurSommet, NullWritable, Message> {

    private static final Logger LOG = Logger.getLogger(CalculFiltre.class);

    @Override
    public void compute(
            Vertex<LongWritable, ValeurSommet, NullWritable> sommet,
            Iterable<Message> messages) throws IOException {

        long superstep = getSuperstep();

        if (superstep == 0) {
            // ── Superstep 0 : envoi des pings aux voisins sortants ─────────
            // NOTE : on ne vote PAS halt ici, afin que les sommets à degré
            // entrant nul (qui ne recevront aucun message S1) restent actifs
            // et puissent agréger leur paire de degrés à la superstep 1.
            Message ping = Message.creerPing(sommet.getId().get());
            sendMessageToAllEdges(sommet, ping);

        } else if (superstep == 1) {
            // ── Superstep 1 : collecte des pings, calcul des degrés ────────
            ValeurSommet valeur = sommet.getValue();
            int degreEntrant = 0;

            for (Message msg : messages) {
                if (msg.estPing()) {
                    degreEntrant++;
                    valeur.ajouterVoisinEntrant(msg.getIdEmetteur());
                }
            }
            valeur.setDegreEntrant(degreEntrant);

            int degreSortant = sommet.getNumEdges();

            // Agréger la paire de degrés pour que le master puisse
            // construire les ensembles de candidats M[u]
            aggregate(
                AggregateurDegresSommets.NOM,
                AggregateurDegresSommets.encoder(
                    sommet.getId().get(), degreEntrant, degreSortant)
            );

            LOG.debug("Sommet " + sommet.getId().get()
                + " : degreEntrant=" + degreEntrant
                + ", degreSortant=" + degreSortant);

        } else {
            // ── Superstep ≥ 2 : transition vers CalculCorrespondance ───────
            // On s'arrête ici pour laisser le Master changer la Computation class
            sommet.voteToHalt();
        }
    }
}
