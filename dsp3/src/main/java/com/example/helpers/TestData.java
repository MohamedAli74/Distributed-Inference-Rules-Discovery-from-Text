package com.example.helpers;

import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.util.*;

/**
 * مسؤول عن:
 * 1) قراءة positive-preds و negative-preds من DistributedCache
 * 2) استخراج الأزواج (predicate1, predicate2) + label
 * 3) توفير Set بكل predicates الموجودة في test-set
 *
 * Supports formats مثل:
 * - "X control Y X prevent Y 0.1566 1"  (زي مثالك)
 * - "X control Y X prevent Y"           (بدون أرقام)
 * - "X control Y\tX prevent Y"          (tabs)
 */
public class TestData {

    // separator داخلي آمن (مش tab ولا space)
    public static final String SEP = "\u0001";

    /** معلومات زوج من التست */
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

    /** Parsing output بسيط */
    public static class PairParsed {
        public final String p1, p2;
        public PairParsed(String p1, String p2) { this.p1 = p1; this.p2 = p2; }
    }

    /** canonical key بحيث (a,b) = (b,a) */
    public static String canonicalPairKey(String a, String b) {
        if (a.compareTo(b) <= 0) return a + SEP + b;
        return b + SEP + a;
    }

    /** هل الفعل auxiliary؟ (عشان Step1 ممكن يتجاهله) */
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

    /**
     * تحميل كل predicates الموجودة في test-set (positive + negative)
     * هذا بنستخدمه مثلاً حتى نفلتر MI/Denom بس لهذول predicates.
     */
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
     * تحميل كل الأزواج (positive=1, negative=0) من cache
     * return Map<canonicalKey, PairInfo(p1,p2,label)>
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
                    // نخزن p1,p2 حسب اللي موجود بالملف + label
                    map.put(key, new PairInfo(pp.p1, pp.p2, label));
                }
            }
        }
        return map;
    }

    /**
     * parser flexible:
     * - إذا فيه tab: pred1 \t pred2 ...
     * - غير هيك: نفصل على spaces ونعتبر كل predicate ينتهي عند token "Y"
     *   (لأن القالب دائماً: X ... Y)
     */
    public static PairParsed parsePairLineFlexible(String line, PorterStemmer stemmer) {
        if (line == null) return null;
        line = line.trim();
        if (line.isEmpty()) return null;

        // إذا فيه tab: نأخذ أول عمودين
        if (line.contains("\t")) {
            String[] f = line.split("\t");
            if (f.length < 2) return null;
            String p1 = normalizePredicate(f[0], stemmer);
            String p2 = normalizePredicate(f[1], stemmer);
            return new PairParsed(p1, p2);
        }

        // spaces format: "X control Y X prevent Y 0.15 1"
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

    /**
     * Normalize predicate to match Step1 templates:
     * - compress spaces
     * - stem verb (2nd token)
     * - keep X and Y
     * - keep prepositions lower-case
     *
     * Examples:
     * "X provide from Y" -> "X provid from Y"
     * "X manages with Y" -> "X manag with Y"
     */
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
        // Hadoop cache: URI may include #positive.txt
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
