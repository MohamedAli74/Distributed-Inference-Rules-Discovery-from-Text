package com.example.helpers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;

public class TestData {

    public static final String SEP = "\t";

    public static class PairInfo {
        public final String p1;
        public final String p2;
        public final int label; // 1 positive, 0 negative
        public PairInfo(String p1, String p2, int label) {
            this.p1 = p1;
            this.p2 = p2;
            this.label = label;
        }
    }

    public static class PairParsed {
        public final String p1, p2;
        public PairParsed(String p1, String p2) { this.p1 = p1; this.p2 = p2; }
    }

    public static String canonicalPairKey(String a, String b) {
        if (a.compareTo(b) <= 0) return a + SEP + b;
        return b + SEP + a;
    }

    public static boolean isAuxiliary(String word, PorterStemmer stemmer) {
        if (word == null) return false;
        String s = stemmer.stem(word.toLowerCase());
        return s.equals("be") || s.equals("am") || s.equals("is") || s.equals("are") || s.equals("was") || s.equals("were")
                || s.equals("been") || s.equals("being")
                || s.equals("do") || s.equals("does") || s.equals("did")
                || s.equals("have") || s.equals("has") || s.equals("had")
                || s.equals("will") || s.equals("would")
                || s.equals("can") || s.equals("could")
                || s.equals("may") || s.equals("might")
                || s.equals("must") || s.equals("shall") || s.equals("should");
    }

    public static Set<String> loadTestPredicates(URI[] cacheFiles, PorterStemmer stemmer) throws IOException {
        Set<String> preds = new HashSet<>();
        if (cacheFiles == null) return preds;

        for (URI uri : cacheFiles) {
            int label = labelFromUri(uri);
            if (label == -1) continue;

            try (BufferedReader br = openCached(uri)) {
                String line;
                while ((line = br.readLine()) != null) {
                    PairParsed pp = parsePairLineFlexible(line, stemmer);
                    if (pp == null) continue;
                    preds.add(pp.p1);
                    preds.add(pp.p2);
                }
            }
        }
        return preds;
    }

    /**
     * IMPORTANT: this is the UNIQUE version (what Step6 expects).
     * Key is canonical, value is a single PairInfo (last wins if duplicates exist).
     */
    public static Map<String, PairInfo> loadPairs(URI[] cacheFiles, PorterStemmer stemmer) throws IOException {
        Map<String, PairInfo> map = new HashMap<>();
        if (cacheFiles == null) return map;

        for (URI uri : cacheFiles) {
            int label = labelFromUri(uri);
            if (label == -1) continue;

            try (BufferedReader br = openCached(uri)) {
                String line;
                while ((line = br.readLine()) != null) {
                    PairParsed pp = parsePairLineFlexible(line, stemmer);
                    if (pp == null) continue;
                    String key = canonicalPairKey(pp.p1, pp.p2);
                    map.put(key, new PairInfo(pp.p1, pp.p2, label));
                }
            }
        }
        return map;
    }

    /**
     * DUPLICATES version (what Step7 needs).
     * Keeps ALL duplicates and preserves orientation as in the files.
     * Key is canonical (so Step6/Step7 similarity lookup matches),
     * but stored PairInfo keeps p1,p2 direction.
     */
    public static Map<String, List<PairInfo>> loadPairsWithDuplicates(URI[] cacheFiles, PorterStemmer stemmer) throws IOException {
        Map<String, List<PairInfo>> map = new HashMap<>();
        if (cacheFiles == null) return map;

        for (URI uri : cacheFiles) {
            int label = labelFromUri(uri);
            if (label == -1) continue;

            try (BufferedReader br = openCached(uri)) {
                String line;
                while ((line = br.readLine()) != null) {
                    PairParsed pp = parsePairLineFlexible(line, stemmer);
                    if (pp == null) continue;

                    String key = canonicalPairKey(pp.p1, pp.p2);
                    map.computeIfAbsent(key, k -> new ArrayList<>())
                       .add(new PairInfo(pp.p1, pp.p2, label));
                }
            }
        }
        return map;
    }

    public static PairParsed parsePairLineFlexible(String line, PorterStemmer stemmer) {
        if (line == null) return null;
        line = line.trim();
        if (line.isEmpty()) return null;

        // Tab-separated: take first 2 columns only
        if (line.contains("\t")) {
            String[] f = line.split("\t");
            if (f.length < 2) return null;
            String p1 = normalizePredicate(f[0], stemmer);
            String p2 = normalizePredicate(f[1], stemmer);
            return new PairParsed(p1, p2);
        }

        String[] toks = line.split("\\s+");

        int y1 = findY(toks, 0);
        if (y1 < 0) return null;
        int y2 = findY(toks, y1 + 1);
        if (y2 < 0) return null;

        String p1raw = join(toks, 0, y1);
        String p2raw = join(toks, y1 + 1, y2);

        String p1 = normalizePredicate(p1raw, stemmer);
        String p2 = normalizePredicate(p2raw, stemmer);
        return new PairParsed(p1, p2);
    }

    private static int findY(String[] toks, int start) {
        for (int i = start; i < toks.length; i++) {
            if (toks[i].equals("Y")) return i;
        }
        return -1;
    }

    private static String join(String[] toks, int from, int toInclusive) {
        StringBuilder sb = new StringBuilder();
        for (int i = from; i <= toInclusive; i++) {
            if (i > from) sb.append(' ');
            sb.append(toks[i]);
        }
        return sb.toString().trim();
    }

    private static String normalizePredicate(String pred, PorterStemmer stemmer) {
        pred = pred.trim().replaceAll("\\s+", " ");
        String[] t = pred.split(" ");
        if (t.length < 3) return pred;

        // expected: X verb (...) Y
        StringBuilder sb = new StringBuilder();
        sb.append("X").append(" ");

        // stem verb (token[1])
        sb.append(stemmer.stem(t[1].toLowerCase()));

        // remaining tokens (prepositions + Y)
        for (int i = 2; i < t.length; i++) {
            sb.append(" ");
            if (t[i].equals("Y")) sb.append("Y");
            else sb.append(t[i].toLowerCase());
        }
        return sb.toString().trim();
    }

    /** Determine label from cache URI name */
    private static int labelFromUri(URI uri) {
        String name = new Path(uri.getPath()).getName();
        if (uri.getFragment() != null) name = uri.getFragment();

        String low = name.toLowerCase();
        if (low.contains("positive")) return 1;
        if (low.contains("negative")) return 0;
        return -1;
    }

    /**
     * In DistributedCache, files appear locally with their fragment name if you used:
     * job.addCacheFile(new URI(path + "#positive.txt"));
     */
    private static BufferedReader openCached(URI uri) throws IOException {
        if (uri.getFragment() != null) {
            return new BufferedReader(new FileReader(uri.getFragment()));
        }
        return new BufferedReader(new FileReader(new File(uri.getPath())));
    }
}
