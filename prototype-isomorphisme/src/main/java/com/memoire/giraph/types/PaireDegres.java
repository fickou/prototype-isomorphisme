package com.memoire.giraph.types;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Paire de degrés (degré entrant, degré sortant) d'un sommet.
 * Utilisée dans la phase de filtrage pour éliminer les candidats
 * incompatibles avec les contraintes de degré du motif.
 */
public class PaireDegres implements Writable {

    private int degreEntrant;
    private int degreSortant;

    public PaireDegres() {
        this.degreEntrant = 0;
        this.degreSortant = 0;
    }

    public PaireDegres(int degreEntrant, int degreSortant) {
        this.degreEntrant = degreEntrant;
        this.degreSortant = degreSortant;
    }

    public int getDegreEntrant()  { return degreEntrant; }
    public int getDegreSortant()  { return degreSortant; }
    public void setDegreEntrant(int d) { this.degreEntrant = d; }
    public void setDegreSortant(int d) { this.degreSortant = d; }

    /**
     * Vérifie si ce sommet du graphe de données peut être candidat
     * pour un sommet du motif ayant les degrés donnés.
     * Condition : degrés de données >= degrés du motif.
     */
    public boolean estCandidatPour(PaireDegres degresMotif) {
        return this.degreEntrant  >= degresMotif.degreEntrant
            && this.degreSortant >= degresMotif.degreSortant;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(degreEntrant);
        out.writeInt(degreSortant);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        degreEntrant = in.readInt();
        degreSortant = in.readInt();
    }

    @Override
    public String toString() {
        return "(" + degreEntrant + "," + degreSortant + ")";
    }

    /** Encodage compact pour les agrégateurs texte : "indeg:outdeg" */
    public String encoder() {
        return degreEntrant + ":" + degreSortant;
    }

    /** Décodage depuis "indeg:outdeg" */
    public static PaireDegres decoder(String s) {
        String[] parts = s.split(":");
        return new PaireDegres(Integer.parseInt(parts[0]),
                               Integer.parseInt(parts[1]));
    }
}
