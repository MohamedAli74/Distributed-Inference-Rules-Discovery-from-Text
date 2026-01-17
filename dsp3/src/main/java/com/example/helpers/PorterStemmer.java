package com.example.helpers;

    
/**
 * Minimal Porter-like stemmer (compact, good enough for DIRT-style outputs):
 * Examples:
 *  provide -> provid
 *  manage  -> manag
 *  involve -> involv
 *  defined -> defin
 *  corrected -> correct
 *
 * This is not a full academic Porter implementation, but works well for
 * assignment-style predicate normalization and matching.
 */
public class PorterStemmer {

    public String stem(String word) {
        if (word == null) return "";
        word = word.toLowerCase();
        if (word.length() <= 2) return word;

        // --- Step 1a: plurals ---
        if (word.endsWith("sses")) word = word.substring(0, word.length() - 2); // sses -> ss
        else if (word.endsWith("ies")) word = word.substring(0, word.length() - 2); // ies -> i
        else if (word.endsWith("ss")) { /* keep */ }
        else if (word.endsWith("s")) word = word.substring(0, word.length() - 1);

        // --- Step 1b: past/gerund (simplified) ---
        if (word.endsWith("eed")) {
            word = word.substring(0, word.length() - 1); // eed -> ee (approx)
        } else if (word.endsWith("ed")) {
            word = word.substring(0, word.length() - 2);
            word = cleanupAfterSuffix(word);
        } else if (word.endsWith("ing")) {
            word = word.substring(0, word.length() - 3);
            word = cleanupAfterSuffix(word);
        }

        // --- Step 1c: y -> i ---
        if (word.endsWith("y") && word.length() > 2) {
            word = word.substring(0, word.length() - 1) + "i";
        }

        // --- A few common Step2/3 suffix reductions (minimal set) ---
        word = stripSuffix(word, "ational", "ate");
        word = stripSuffix(word, "tional", "tion");
        word = stripSuffix(word, "ization", "ize");
        word = stripSuffix(word, "iveness", "ive");
        word = stripSuffix(word, "fulness", "ful");
        word = stripSuffix(word, "ousness", "ous");
        word = stripSuffix(word, "biliti", "ble");
        word = stripSuffix(word, "icate", "ic");
        word = stripSuffix(word, "ative", "");
        word = stripSuffix(word, "alize", "al");
        word = stripSuffix(word, "iciti", "ic");
        word = stripSuffix(word, "ical", "ic");
        word = stripSuffix(word, "ness", "");
        word = stripSuffix(word, "ment", "");

        // --- small final tweaks ---
        if (word.endsWith("tion")) word = word.substring(0, word.length() - 3) + "t"; // tion -> t
        if (word.endsWith("sion")) word = word.substring(0, word.length() - 3) + "s"; // sion -> s

        if (word.endsWith("e") && word.length() > 3) {
            word = word.substring(0, word.length() - 1);
        }

        return word;
    }

    private static String stripSuffix(String w, String suf, String rep) {
        if (w.endsWith(suf) && w.length() > suf.length() + 1) {
            return w.substring(0, w.length() - suf.length()) + rep;
        }
        return w;
    }

    /**
     * After removing "ed" or "ing" we do some basic cleanup to better match common stems:
     * - at/bl/iz endings -> add 'e' (e.g., "operat"->"operate" then later may drop 'e')
     * - double consonant trimming (e.g., "stopp"->"stop")
     */
    private static String cleanupAfterSuffix(String w) {
        if (w.length() <= 2) return w;

        if (w.endsWith("at") || w.endsWith("bl") || w.endsWith("iz")) {
            w = w + "e";
        }

        // trim double consonant at the end: tt, ss, pp, etc. (keep "ss" sometimes)
        if (w.length() >= 2) {
            char a = w.charAt(w.length() - 1);
            char b = w.charAt(w.length() - 2);
            if (a == b && isConsonant(a)) {
                // keep 'ss' case sometimes, but it's fine to trim for our usage
                w = w.substring(0, w.length() - 1);
            }
        }
        return w;
    }

    private static boolean isConsonant(char c) {
        return "aeiou".indexOf(c) < 0;
    }
}
