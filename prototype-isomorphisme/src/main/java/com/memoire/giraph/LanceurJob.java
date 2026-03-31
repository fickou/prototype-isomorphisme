package com.memoire.giraph;

import com.memoire.giraph.calcul.CalculFiltre;
import com.memoire.giraph.calcul.MaitreUnique;
import com.memoire.giraph.es.FormatEntreeListeAdj;
import com.memoire.giraph.types.Motif;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Point d'entrée principal pour lancer le job Giraph d'isomorphisme de sous-graphe.
 *
 * <p>Arguments de ligne de commande :
 * <ul>
 *   <li>{@code arg[0]} : chemin HDFS du graphe de données (VertexInputFormat)</li>
 *   <li>{@code arg[1]} : chemin HDFS du motif (paramètre de config)</li>
 *   <li>{@code arg[2]} : chemin HDFS du fichier de résultats (écrit par le master)</li>
 *   <li>{@code arg[3]} : nombre de workers (optionnel, défaut 1)</li>
 * </ul>
 */
public class LanceurJob extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(LanceurJob.class);

    @Override
    public int run(String[] args) throws Exception {

        System.out.println("Nb args = " + args.length);
        for (int i = 0; i < args.length; i++) {
            System.out.println("arg[" + i + "] = " + args[i]);
        }

        if (args.length > 0 && args[0].equals(this.getClass().getName())) {
            String[] newArgs = new String[args.length - 1];
            System.arraycopy(args, 1, newArgs, 0, args.length - 1);
            args = newArgs;
        }

        if (args.length < 3 || args.length > 4) {
            System.err.println("Usage : LanceurJob <chemin_donnees> <chemin_motif> <chemin_resultats> [nb_workers]");
            return -2;
        }

        String chemDonnees   = args[0];
        String chemMotif     = args[1];
        String chemResultats = args[2];

        int nbWorkers = 1;
        if (args.length == 4) {
            try {
                nbWorkers = Integer.parseInt(args[3]);
                if (nbWorkers <= 0) {
                    System.err.println("nb_workers doit être un entier positif, trouvé : " + args[3]);
                    return -2;
                }
            } catch (NumberFormatException e) {
                System.err.println("nb_workers doit être un entier, trouvé : " + args[3]);
                return -2;
            }
        }

        GiraphConfiguration conf = new GiraphConfiguration(getConf());

        // Correction : Permettre des retries pour éviter les échecs temporaires
        conf.setInt("mapred.map.max.attempts", 3);
        conf.setInt("mapred.reduce.max.attempts", 3);

        // Correction : Augmenter la mémoire par tâche pour éviter les erreurs de ressources
        conf.set("mapred.child.java.opts", "-Xmx1024m");

        conf.setWorkerConfiguration(nbWorkers, nbWorkers, 100.0f);

        // Classes principales
        conf.setComputationClass(CalculFiltre.class);
        conf.setMasterComputeClass(MaitreUnique.class);
        conf.setVertexInputFormatClass(FormatEntreeListeAdj.class);

        // Note : les types (I=LongWritable, V=ValeurSommet, E=NullWritable, M=Message)
        // sont inférés automatiquement depuis les paramètres génériques de CalculFiltre.

        // Paramètres personnalisés pour l'application
        conf.set(Motif.CLE_CHEMIN_MOTIF, chemMotif);
        conf.set("resultats.chemin", chemResultats);

        GiraphFileInputFormat.addVertexInputPath(conf, new Path(chemDonnees));
        GiraphJob job = new GiraphJob(conf, "Job Isomorphisme SSP (Ullmann) : " + chemDonnees);

        LOG.info("Lancement du job avec : ");
        LOG.info("- Données : " + chemDonnees);
        LOG.info("- Motif   : " + chemMotif);
        LOG.info("- Sortie  : " + chemResultats);
        LOG.info("- Workers : " + nbWorkers);

        // Correction : Ajouter gestion d'erreurs et logs détaillés pour déboguer
        try {
            boolean res = job.run(true);
            LOG.info("Le job Giraph " + (res ? "a réussi" : "a échoué") + ".");
            return res ? 0 : -1;
        } catch (Exception e) {
            LOG.error("Erreur lors de l'exécution du job : ", e);
            e.printStackTrace();  // Pour capturer les détails
            return -1;
        }
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new LanceurJob(), args);
        System.exit(result);
    }
}