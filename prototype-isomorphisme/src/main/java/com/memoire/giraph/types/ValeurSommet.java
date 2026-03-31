package com.memoire.giraph.types;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Valeur associée à chaque sommet du graphe de données dans Giraph.
 *
 * <p>Contient :
 * <ul>
 *   <li>Le degré entrant (calculé à la superstep 1 de la phase filtre)</li>
 *   <li>La liste des voisins entrants (nécessaire pour la vérification
 *       structurelle des arcs dans la phase de correspondance)</li>
 * </ul>
 *
 * <p>Note : les voisins sortants sont directement accessibles via
 * {@code vertex.getEdges()} dans Giraph et ne sont pas dupliqués ici.
 */
public class ValeurSommet implements Writable {

    /** Degré entrant du sommet (nombre d'arcs entrants). */
    private int degreEntrant;

    /**
     * Identifiants des voisins entrants (sommets qui ont un arc vers ce sommet).
     * Collectés durant la superstep 1 de la phase filtre.
     */
    private List<Long> voisinsEntrants;

    public ValeurSommet() {
        this.degreEntrant   = 0;
        this.voisinsEntrants = new ArrayList<>();
    }

    // ── Accesseurs ───────────────────────────────────────────────────────────

    public int getDegreEntrant()               { return degreEntrant; }
    public void setDegreEntrant(int d)         { this.degreEntrant = d; }
    public List<Long> getVoisinsEntrants()     { return voisinsEntrants; }

    /** Ajoute un voisin entrant lors de la collecte en superstep 1. */
    public void ajouterVoisinEntrant(long id) {
        if (!voisinsEntrants.contains(id)) {
            voisinsEntrants.add(id);
        }
    }

    // ── Sérialisation Writable ────────────────────────────────────────────────

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(degreEntrant);
        out.writeInt(voisinsEntrants.size());
        for (long id : voisinsEntrants) {
            out.writeLong(id);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        degreEntrant = in.readInt();
        int taille = in.readInt();
        voisinsEntrants = new ArrayList<>(taille);
        for (int i = 0; i < taille; i++) {
            voisinsEntrants.add(in.readLong());
        }
    }

    @Override
    public String toString() {
        return "ValeurSommet{degreEntrant=" + degreEntrant
             + ", voisinsEntrants=" + voisinsEntrants + "}";
    }
}
