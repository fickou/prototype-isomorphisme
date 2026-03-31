package com.memoire.giraph.types;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Message échangé entre les sommets du graphe de données dans Giraph.
 *
 * <p>Deux types de messages sont utilisés :
 * <ul>
 *   <li>{@link #PING_DEGRE} (superstep 0) : signal envoyé à tous les voisins
 *       sortants afin que le destinataire puisse calculer son degré entrant.</li>
 *   <li>{@link #EXPLORATION} (superstep ≥ 2) : transport d'un mapping partiel
 *       motif → données, encodé sous la forme {@code "u0:v0,u1:v1,..."}</li>
 * </ul>
 *
 * <p>Format du mapping partiel : chaque entrée est "idSommetMotif:idSommetDonnees",
 * séparées par des virgules. Exemple : {@code "0:3,1:7,2:5"}.
 */
public class Message implements Writable {

    /** Type : signal de degré envoyé en superstep 0. */
    public static final int PING_DEGRE  = 0;

    /** Type : mapping partiel envoyé en phase de correspondance. */
    public static final int EXPLORATION = 1;

    private int    type;
    private long   idEmetteur;    // identifiant du sommet émetteur
    private String mappingEncode; // non null si type == EXPLORATION

    public Message() {
        this.type         = PING_DEGRE;
        this.idEmetteur   = -1L;
        this.mappingEncode = "";
    }

    // ── Constructeurs de commodité ────────────────────────────────────────────

    /** Crée un message PING_DEGRE (l'émetteur indique son id). */
    public static Message creerPing(long idEmetteur) {
        Message m = new Message();
        m.type       = PING_DEGRE;
        m.idEmetteur = idEmetteur;
        return m;
    }

    /** Crée un message EXPLORATION portant un mapping partiel encodé. */
    public static Message creerExploration(long idEmetteur, String mappingEncode) {
        Message m = new Message();
        m.type         = EXPLORATION;
        m.idEmetteur   = idEmetteur;
        m.mappingEncode = mappingEncode;
        return m;
    }

    // ── Accesseurs ────────────────────────────────────────────────────────────

    public int    getType()          { return type; }
    public long   getIdEmetteur()    { return idEmetteur; }
    public String getMappingEncode() { return mappingEncode; }

    public boolean estPing()        { return type == PING_DEGRE; }
    public boolean estExploration() { return type == EXPLORATION; }

    // ── Sérialisation Writable ────────────────────────────────────────────────

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(type);
        out.writeLong(idEmetteur);
        out.writeUTF(mappingEncode == null ? "" : mappingEncode);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        type         = in.readInt();
        idEmetteur   = in.readLong();
        mappingEncode = in.readUTF();
    }

    @Override
    public String toString() {
        if (type == PING_DEGRE) {
            return "Message{PING de " + idEmetteur + "}";
        }
        return "Message{EXPLORATION de " + idEmetteur
             + ", mapping=" + mappingEncode + "}";
    }
}
