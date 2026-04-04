package com.memoire.giraph.calcul;

import com.memoire.giraph.agregateur.AggregateurDegresSommets;
import com.memoire.giraph.types.Message;
import com.memoire.giraph.types.ValeurSommet;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import java.io.IOException;


// Phase 1 — Filtrage : calcul des degrés des sommets du graphe de données.

public class CalculFiltre
        extends BasicComputation<LongWritable, ValeurSommet, NullWritable, Message> {

    private static final Logger LOG = Logger.getLogger(CalculFiltre.class);

    @Override
    public void compute(
            Vertex<LongWritable, ValeurSommet, NullWritable> sommet,
            Iterable<Message> messages) throws IOException {

        long superstep = getSuperstep();

        if (superstep == 0) {
            // Superstep 0 : envoi des pings aux voisins sortants
            Message ping = Message.creerPing(sommet.getId().get());
            sendMessageToAllEdges(sommet, ping);

        } else if (superstep == 1) {
            // Superstep 1 : collecte des pings, calcul des degrés
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

            // Agrégation de la paire de degrés pour que le master puisse construire les ensembles de candidats M[u]
            aggregate(
                AggregateurDegresSommets.NOM,
                AggregateurDegresSommets.encoder(
                    sommet.getId().get(), degreEntrant, degreSortant)
            );

            LOG.debug("Sommet " + sommet.getId().get()
                + " : degreEntrant = " + degreEntrant
                + ", degreSortant = " + degreSortant);

        } else {
            sommet.voteToHalt();
        }
    }
}
