package com.memoire.giraph.types;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// Valeur associée à chaque sommet du graphe de données dans Giraph.
public class ValeurSommet implements Writable {

    private int degreEntrant;

    private List<Long> voisinsEntrants;

    public ValeurSommet() {
        this.degreEntrant   = 0;
        this.voisinsEntrants = new ArrayList<>();
    }

    // Accesseurs
    public int getDegreEntrant()               { return degreEntrant; }
    public void setDegreEntrant(int d)         { this.degreEntrant = d; }
    public List<Long> getVoisinsEntrants()     { return voisinsEntrants; }

    public void ajouterVoisinEntrant(long id) {
        if (!voisinsEntrants.contains(id)) {
            voisinsEntrants.add(id);
        }
    }

    // Sérialisation Writable
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
