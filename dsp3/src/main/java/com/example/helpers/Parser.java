package com.example.helpers;

import java.util.*;

/**
 * Parser utilities for Google Syntactic N-Grams "biarc" lines.
 *
 * A biarc line looks like:
 *   control/VB/ROOT/0 drug/NN/nsubj/1 pain/NN/dobj/1 10 2005,10
 *
 * Each token has the form:
 *   word / POS / depLabel / headIndex
 *
 * After all tokens, the first pure integer is the COUNT (frequency),
 * and after that you may have year,decade counts (we ignore them here).
 *
 * This class helps Step1:
 *  - parse a biarc line into Token objects + count
 *  - find the ROOT verb
 *  - extract a predicate "template" and the X/Y argument words (stemmed)
 *
 * Template examples:
 *  - "X control Y"
 *  - "X provid from Y"    (verb + preposition)
 */
public class Parser {

    /**
     * Result of parsing a biarc line:
     *  - tokens: dependency tokens (with indices, POS, dep label, head index)
     *  - count : the first integer in the line (frequency of this pattern)
     */
    public static class ParsedLine {
        public final List<Token> tokens;
        public final int count;

        public ParsedLine(List<Token> tokens, int count) {
            this.tokens = tokens;
            this.count = count;
        }
    }

    /**
     * What Step1 needs to output (conceptually):
     *  - template: generalized predicate template using X and Y
     *  - xWordStem: stemmed noun for the subject slot (X)
     *  - yWordStem: stemmed noun for the object slot (Y)
     */
    public static class PredicateInstance {
        public final String template;   // "X verb Y" OR "X verb prep Y"
        public final String xWordStem;
        public final String yWordStem;

        public PredicateInstance(String template, String xWordStem, String yWordStem) {
            this.template = template;
            this.xWordStem = xWordStem;
            this.yWordStem = yWordStem;
        }
    }

    /**
     * Parse a raw biarc line.
     *
     * Example:
     *   control/VB/ROOT/0 X/NN/nsubj/1 Y/NN/dobj/1 10 1999,2 2005,8
     *
     * We read tokens up until the first purely-numeric field (e.g., "10"),
     * which is treated as the COUNT. Everything after COUNT (year stats) is ignored.
     *
     * Note:
     *  - We store token indices as 1-based indices, because head indices in the dataset
     *    are typically 1-based (verb token usually has index 1).
     */
    public static ParsedLine parseLine(String line) {
        if (line == null) return null;
        line = line.trim();
        if (line.isEmpty()) return null;

        String[] parts = line.split("\\s+");

        // Find the first token that is purely digits -> count
        int countIdx = -1;
        for (int i = 0; i < parts.length; i++) {
            if (parts[i].matches("\\d+")) {
                countIdx = i;
                break;
            }
        }
        if (countIdx < 0) return null;

        int count;
        try {
            count = Integer.parseInt(parts[countIdx]);
        } catch (Exception e) {
            return null;
        }

        List<Token> toks = new ArrayList<>();
        int idx = 1; // 1-based token index
        for (int i = 0; i < countIdx; i++) {
            Token t = parseToken(parts[i], idx);
            if (t != null) {
                toks.add(t);
                idx++;
            }
        }

        if (toks.isEmpty()) return null;
        return new ParsedLine(toks, count);
    }

    /**
     * Parse a single token in the format:
     *   word/POS/dep/head
     *
     * Examples:
     *   control/VB/ROOT/0
     *   drug/NN/nsubj/1
     *   from/IN/prep/1
     *
     * Warning:
     *   Sometimes the word itself can contain '/' characters.
     *   Therefore we parse from the end using lastIndexOf('/') repeatedly.
     */
    private static Token parseToken(String s, int idx) {
        if (s == null) return null;

        // Last part is head index
        int last = s.lastIndexOf('/');
        if (last < 0) return null;
        String headStr = s.substring(last + 1);

        int head;
        try {
            head = Integer.parseInt(headStr);
        } catch (Exception e) {
            return null;
        }

        String rest = s.substring(0, last);

        // Next is dependency label
        int last2 = rest.lastIndexOf('/');
        if (last2 < 0) return null;
        String dep = rest.substring(last2 + 1);
        rest = rest.substring(0, last2);

        // Next is POS tag
        int last3 = rest.lastIndexOf('/');
        if (last3 < 0) return null;
        String pos = rest.substring(last3 + 1);

        // Remaining prefix is the word
        String word = rest.substring(0, last3);

        return new Token(idx, word, pos, dep, head);
    }

    /**
     * Find the main ROOT verb.
     *
     * Strategy:
     * 1) Prefer a token that is ROOT with head=0 and POS starting with "VB"
     * 2) Fallback: any ROOT with head=0 (even if POS is not VB)
     */
    public static Token findRootVerb(List<Token> toks) {
        if (toks == null) return null;

        for (Token t : toks) {
            if ("ROOT".equals(t.dep) && t.head == 0 && isVerbPos(t.pos)) return t;
        }

        for (Token t : toks) {
            if ("ROOT".equals(t.dep) && t.head == 0) return t;
        }

        return null;
    }

    /** True if POS tag is a verb POS (VB, VBD, VBG, VBN, VBP, VBZ...) */
    private static boolean isVerbPos(String pos) {
        return pos != null && pos.startsWith("VB");
    }

    /** True if POS tag is noun-ish (NN*, or PRP as a simple fallback) */
    private static boolean isNounPos(String pos) {
        if (pos == null) return false;
        return pos.startsWith("NN") || pos.equals("PRP");
    }

    /**
     * Find a child token that has a given dependency label from a given head index.
     *
     * Example:
     *  - root.idx=1
     *  - findChild(tokens, 1, "nsubj") returns subject
     */
    private static Token findChild(List<Token> toks, int headIdx, String dep) {
        for (Token t : toks) {
            if (t.head == headIdx && dep.equals(t.dep)) return t;
        }
        return null;
    }

    /** Try multiple dependency labels in order and return the first matching child. */
    private static Token findChildAny(List<Token> toks, int headIdx, String... deps) {
        for (String d : deps) {
            Token t = findChild(toks, headIdx, d);
            if (t != null) return t;
        }
        return null;
    }

    /**
     * Extract a predicate instance (template + X/Y argument words) from the parsed tokens.
     *
     * We expect:
     *  - X to be a nominal subject of the root verb (nsubj / nsubjpass)
     *  - Y to be either:
     *      (A) direct object (dobj), OR
     *      (B) prepositional object: root --prep--> prepToken --pobj--> noun
     *
     * Output templates:
     *  - "X <verbStem> Y"
     *  - "X <verbStem> <prep> Y"   (when using the prep+pobj case)
     *
     * Stemming:
     *  - We stem the verb and both argument words.
     *  - Preposition is kept lower-case (not stemmed).
     */
    public static Optional<PredicateInstance> extractPredicate(
            List<Token> toks,
            Token root,
            PorterStemmer stemmer
    ) {
        if (toks == null || root == null) return Optional.empty();

        // X = subject (nsubj or nsubjpass)
        Token x = findChildAny(toks, root.idx, "nsubj", "nsubjpass");
        if (x == null || !isNounPos(x.pos)) return Optional.empty();

        // Y = direct object if exists
        Token y = findChild(toks, root.idx, "dobj");
        if (y != null && !isNounPos(y.pos)) y = null;

        String prepWord = null;

        // If no dobj, try: root --prep--> (IN/TO) --pobj--> noun
        if (y == null) {
            Token prep = findChild(toks, root.idx, "prep");
            if (prep != null && ("IN".equals(prep.pos) || "TO".equals(prep.pos))) {
                Token pobj = findChildAny(toks, prep.idx, "pobj", "pcomp", "dobj");
                if (pobj != null && isNounPos(pobj.pos)) {
                    y = pobj;
                    prepWord = prep.word.toLowerCase();
                }
            }
        }

        if (y == null) return Optional.empty();

        // Stem verb and argument words
        String verbStem = stemmer.stem(root.word.toLowerCase());
        String xStem = stemmer.stem(x.word.toLowerCase());
        String yStem = stemmer.stem(y.word.toLowerCase());

        // Build template
        String template;
        if (prepWord == null) {
            template = "X " + verbStem + " Y";
        } else {
            template = "X " + verbStem + " " + prepWord + " Y";
        }

        return Optional.of(new PredicateInstance(template, xStem, yStem));
    }
}
