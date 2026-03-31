package com.memoire.giraph.types;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Représentation du graphe de motif (pattern graph).
 *
 * <p>Le motif est un graphe orienté chargé depuis un fichier texte HDFS.
 * Format : une ligne par sommet, {@code "id voisin1 voisin2 ..."}.
 * Un sommet sans voisins sortants peut apparaître seul : {@code "id"}.
 *
 * <p>Cette classe est chargée une fois par worker via
 * {@link #charger(Configuration)} et mise en cache (singleton par JVM).
 */
public class Motif {

    private static final Logger LOG = Logger.getLogger(Motif.class);

    /** Clé de configuration pour le chemin HDFS du fichier motif. */
    public static final String CLE_CHEMIN_MOTIF = "motif.chemin";

    // ── Singleton par JVM (chaque worker charge une seule fois) ──────────────

    private static volatile Motif instance = null;

    // ── Structure du motif ────────────────────────────────────────────────────

    /** Adjacence sortante : sommetMotif → liste de successeurs. */
    private final Map<Long, Set<Long>> voisinsSortants;

    /** Adjacence entrante : sommetMotif → liste de prédécesseurs. */
    private final Map<Long, Set<Long>> voisinsEntrants;

    /** Degrés de chaque sommet du motif. */
    private final Map<Long, PaireDegres> degres;

    /** Tous les identifiants des sommets du motif. */
    private final List<Long> sommets;

    /** Ordre d'exploration DFS depuis la racine (calculé au chargement). */
    private List<Long> ordreExploration;

    // ── Constructeur ──────────────────────────────────────────────────────────

    private Motif() {
        this.voisinsSortants  = new LinkedHashMap<>();
        this.voisinsEntrants  = new LinkedHashMap<>();
        this.degres           = new LinkedHashMap<>();
        this.sommets          = new ArrayList<>();
        this.ordreExploration = new ArrayList<>();
    }

    // ── Chargement depuis HDFS ────────────────────────────────────────────────

    /**
     * Charge le motif depuis le fichier HDFS désigné par
     * {@link #CLE_CHEMIN_MOTIF} dans la configuration.
     * La méthode est thread-safe et ne charge le fichier qu'une fois.
     *
     * @param conf configuration Hadoop/Giraph
     * @return instance unique du motif
     * @throws IOException si le fichier ne peut pas être lu
     */
    public static Motif charger(Configuration conf) throws IOException {
        if (instance == null) {
            synchronized (Motif.class) {
                if (instance == null) {
                    String chemin = conf.get(CLE_CHEMIN_MOTIF);
                    if (chemin == null || chemin.isEmpty()) {
                        throw new IOException(
                            "Paramètre manquant : " + CLE_CHEMIN_MOTIF);
                    }
                    LOG.info("Chargement du motif depuis : " + chemin);
                    instance = lireDepuisHDFS(conf, chemin);
                    LOG.info("Motif chargé : " + instance.sommets.size()
                             + " sommets, " + instance.compterArcs() + " arcs.");
                }
            }
        }
        return instance;
    }

    /** Réinitialise le singleton (utile pour les tests unitaires). */
    public static void reinitialiser() {
        instance = null;
    }

    private static Motif lireDepuisHDFS(Configuration conf, String chemin)
            throws IOException {

        Motif motif = new Motif();
        FileSystem fs = FileSystem.get(conf);

        try (BufferedReader lecteur = new BufferedReader(
                new InputStreamReader(fs.open(new Path(chemin))))) {

            String ligne;
            while ((ligne = lecteur.readLine()) != null) {
                ligne = ligne.replace("\uFEFF", "").trim();
                if (ligne.isEmpty() || ligne.startsWith("#")) {
                    continue; // ignorer commentaires et lignes vides
                }
                String[] tokens = ligne.split("\\s+");
                long idSommet = Long.parseLong(tokens[0]);

                // Enregistrer le sommet s'il n'existe pas encore
                if (!motif.voisinsSortants.containsKey(idSommet)) {
                    motif.voisinsSortants.put(idSommet, new LinkedHashSet<>());
                    motif.voisinsEntrants.put(idSommet, new LinkedHashSet<>());
                    motif.sommets.add(idSommet);
                }

                // Enregistrer les arcs sortants
                for (int i = 1; i < tokens.length; i++) {
                    long idVoisin = Long.parseLong(tokens[i]);
                    motif.voisinsSortants.get(idSommet).add(idVoisin);

                    // Créer le voisin s'il n'existe pas encore
                    if (!motif.voisinsEntrants.containsKey(idVoisin)) {
                        motif.voisinsSortants.put(idVoisin, new LinkedHashSet<>());
                        motif.voisinsEntrants.put(idVoisin, new LinkedHashSet<>());
                        motif.sommets.add(idVoisin);
                    }
                    motif.voisinsEntrants.get(idVoisin).add(idSommet);
                }
            }
        }

        // Calculer les degrés
        for (long s : motif.sommets) {
            int dSortant = motif.voisinsSortants.get(s).size();
            int dEntrant = motif.voisinsEntrants.get(s).size();
            motif.degres.put(s, new PaireDegres(dEntrant, dSortant));
        }

        // Calculer l'ordre d'exploration par DFS depuis le premier sommet
        if (!motif.sommets.isEmpty()) {
            motif.ordreExploration = motif.calculerOrdreDFS(motif.sommets.get(0));
        }

        return motif;
    }

    // ── Algorithme DFS pour l'ordre d'exploration ─────────────────────────────

    /**
     * Calcule un ordre d'exploration DFS du motif depuis un sommet racine.
     *
     * <p>Attention : un ordre DFS simple ne garantit pas, pour un motif général,
     * que chaque sommet u_{k+1} soit adjacent au sommet précédent u_k dans la
     * liste. Cette propriété est donc vérifiée séparément par
     * {@link #ordreEstLineairementExplorable(List)} avant de lancer la phase de
     * correspondance du prototype.
     */
    public List<Long> calculerOrdreDFS(long racine) {
        List<Long> ordre = new ArrayList<>();
        Set<Long>  visite = new LinkedHashSet<>();
        dfsParcours(racine, visite, ordre);
        // Ajouter les sommets non atteints (composantes non connexes)
        for (long s : sommets) {
            if (!visite.contains(s)) {
                dfsParcours(s, visite, ordre);
            }
        }
        return ordre;
    }

    private void dfsParcours(long u, Set<Long> visite, List<Long> ordre) {
        visite.add(u);
        ordre.add(u);
        // Suivre d'abord les arcs sortants
        for (long v : voisinsSortants.getOrDefault(u, Collections.emptySet())) {
            if (!visite.contains(v)) {
                dfsParcours(v, visite, ordre);
            }
        }
        // Puis les arcs entrants (pour la généralité)
        for (long v : voisinsEntrants.getOrDefault(u, Collections.emptySet())) {
            if (!visite.contains(v)) {
                dfsParcours(v, visite, ordre);
            }
        }
    }

    // ── Vérification structurelle ─────────────────────────────────────────────

    /**
     * Vérifie si le mapping partiel étendu avec (idMotif → idDonnees) est
     * structurellement cohérent avec le motif pour toutes les arêtes impliquant
     * {@code idMotif} et les sommets déjà mappés.
     *
     * @param mappingActuel mapping partiel existant (motif → données)
     * @param idMotif       identifiant du sommet motif à vérifier
     * @param idDonnees     identifiant du sommet données candidat
     * @param voisinsSortantsDonnees  ensemble des voisins sortants du candidat
     * @param voisinsEntrantsDonnees  liste des voisins entrants du candidat
     */
    public boolean verifierCoherence(
            Map<Long, Long> mappingActuel,
            long idMotif,
            long idDonnees,
            Set<Long> voisinsSortantsDonnees,
            List<Long> voisinsEntrantsDonnees) {

        // Vérifier injectivité : idDonnees pas déjà utilisé
        if (mappingActuel.containsValue(idDonnees)) {
            return false;
        }

        Set<Long> vEntrants = new HashSet<>(voisinsEntrantsDonnees);

        // Pour chaque sommet du motif déjà mappé
        for (Map.Entry<Long, Long> entree : mappingActuel.entrySet()) {
            long uDejaMappe  = entree.getKey();
            long vDejaMappe  = entree.getValue();

            // Si arc u_mappé → idMotif dans le motif :
            // il doit exister v_mappé → idDonnees dans les données
            if (aArc(uDejaMappe, idMotif)) {
                if (!vEntrants.contains(vDejaMappe)) {
                    return false;
                }
            }

            // Si arc idMotif → u_mappé dans le motif :
            // il doit exister idDonnees → v_mappé dans les données
            if (aArc(idMotif, uDejaMappe)) {
                if (!voisinsSortantsDonnees.contains(vDejaMappe)) {
                    return false;
                }
            }
        }

        return true;
    }

    // ── Accesseurs ────────────────────────────────────────────────────────────

    public List<Long> getSommets()                 { return Collections.unmodifiableList(sommets); }
    public int        getNombreSommets()           { return sommets.size(); }
    public List<Long> getOrdreExploration()        { return Collections.unmodifiableList(ordreExploration); }
    public PaireDegres getDegres(long id)          { return degres.get(id); }
    public Set<Long>  getVoisinsSortants(long id)  { return voisinsSortants.getOrDefault(id, Collections.emptySet()); }
    public Set<Long>  getVoisinsEntrants(long id)  { return voisinsEntrants.getOrDefault(id, Collections.emptySet()); }

    public boolean aArc(long source, long destination) {
        Set<Long> sortants = voisinsSortants.get(source);
        return sortants != null && sortants.contains(destination);
    }

    /** Retourne vrai si deux sommets du motif sont adjacents dans au moins un sens. */
    public boolean sontAdjacents(long a, long b) {
        return aArc(a, b) || aArc(b, a);
    }

    /**
     * Vérifie que l'ordre d'exploration est compatible avec la stratégie actuelle
     * du prototype : chaque nouveau sommet doit pouvoir être atteint à partir du
     * sommet mappé à l'étape précédente.
     */
    public boolean ordreEstLineairementExplorable(List<Long> ordre) {
        if (ordre == null || ordre.size() <= 1) {
            return true;
        }
        for (int i = 1; i < ordre.size(); i++) {
            if (!sontAdjacents(ordre.get(i - 1), ordre.get(i))) {
                return false;
            }
        }
        return true;
    }

    public long compterArcs() {
        return voisinsSortants.values().stream().mapToLong(Set::size).sum();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Motif{\n");
        for (long s : sommets) {
            sb.append("  ").append(s).append(" → ")
              .append(voisinsSortants.get(s)).append("\n");
        }
        sb.append("  Ordre DFS : ").append(ordreExploration).append("\n}");
        return sb.toString();
    }
}
