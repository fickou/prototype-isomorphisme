package com.memoire.giraph.es;

import com.memoire.giraph.types.ValeurSommet;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Format d'entrée pour lire un graphe sous forme de liste d'adjacence textuelle.
 * 
 * <p>Format attendu par ligne : {@code idSommet idVoisinSortant1 idVoisinSortant2 ...}
 * <br>Exemple : {@code 1 2 3} signifie que le sommet 1 a des arcs dirigés vers 2 et 3.
 * Un sommet sans voisins sortants peut être défini par son ID seul : {@code 4}.
 */
public class FormatEntreeListeAdj extends TextVertexInputFormat<LongWritable, ValeurSommet, NullWritable> {

    @Override
    public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) {
        return new LecteurSommetListeAdj();
    }

    /**
     * Lecteur personnalisé qui gère manuellement les sauts de lignes commentées ou vides.
     */
    protected class LecteurSommetListeAdj extends TextVertexReader {

        private Vertex<LongWritable, ValeurSommet, NullWritable> currentVertex = null;

        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            while (getRecordReader().nextKeyValue()) {
                Text ligne = getRecordReader().getCurrentValue();
                
                // Enlever le BOM UTF-8 éventuel (\uFEFF) et espaces inutiles
                String spec = ligne.toString().replace("\uFEFF", "").trim();
                
                if (spec.isEmpty() || spec.startsWith("#")) {
                    continue; // Ignorer la ligne et lire la suivante
                }

                String[] tokens = spec.split("\\s+");
                long id = Long.parseLong(tokens[0]);

                List<Edge<LongWritable, NullWritable>> arcs = new ArrayList<>();
                for (int i = 1; i < tokens.length; i++) {
                    arcs.add(EdgeFactory.create(new LongWritable(Long.parseLong(tokens[i])), NullWritable.get()));
                }

                Vertex<LongWritable, ValeurSommet, NullWritable> vertex = getConf().createVertex();
                vertex.initialize(new LongWritable(id), new ValeurSommet(), arcs);
                
                currentVertex = vertex;
                return true;
            }
            return false;
        }

        @Override
        public Vertex<LongWritable, ValeurSommet, NullWritable> getCurrentVertex() throws IOException, InterruptedException {
            return currentVertex;
        }
    }
}
