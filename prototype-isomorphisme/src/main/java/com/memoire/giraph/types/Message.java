package com.memoire.giraph.types;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Message échangé entre les sommets du graphe de données dans Giraph.

public class Message implements Writable {

    public static final int PING_DEGRE  = 0;

    public static final int EXPLORATION = 1;

    private int    type;
    private long   idEmetteur;    
    private String mappingEncode; 

    public Message() {
        this.type         = PING_DEGRE;
        this.idEmetteur   = -1L;
        this.mappingEncode = "";
    }

    // Constructeurs de commodité

    public static Message creerPing(long idEmetteur) {
        Message m = new Message();
        m.type       = PING_DEGRE;
        m.idEmetteur = idEmetteur;
        return m;
    }

    public static Message creerExploration(long idEmetteur, String mappingEncode) {
        Message m = new Message();
        m.type         = EXPLORATION;
        m.idEmetteur   = idEmetteur;
        m.mappingEncode = mappingEncode;
        return m;
    }

    // Accesseurs

    public int    getType()          { return type; }
    public long   getIdEmetteur()    { return idEmetteur; }
    public String getMappingEncode() { return mappingEncode; }

    public boolean estPing()        { return type == PING_DEGRE; }
    public boolean estExploration() { return type == EXPLORATION; }

    // Sérialisation Writable

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
