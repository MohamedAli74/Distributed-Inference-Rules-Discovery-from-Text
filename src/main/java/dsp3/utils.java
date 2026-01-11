package dsp3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import dsp3.Stemmer; 


public class utils {

    // =================================================================
    // NEW STATIC HELPER METHOD FOR YOUR ASSIGNMENT
    // =================================================================
    public static String stem(String word) {
        Stemmer s = new Stemmer();
        char[] chars = word.toCharArray();
        s.add(chars, chars.length);
        s.stem();
        return s.toString();
    }
    // =================================================================

    private static final HashSet<String> stopVerbs = new HashSet<String>(Arrays.asList(
        "be", "am", "is", "are", "was", "were", "been", "being",
        "have", "has", "had",
        "do", "does", "did",
        "will", "would", "shall", "should", "can", "could", "may", "might", "must"
    ));

    public static boolean isStopVerb(String verb){
        return stopVerbs.contains(verb.toLowerCase());
    }

    public static parsedLine parse(String line) {
        String[] fields = line.split("\t");
        //[head_word, syntactic-ngram, total_count, counts_by_year]

        String[] tokens = (fields[1].split(" "));
        //Word / POS / Relation / Parent-ID
        word[] words = new word[tokens.length]; 
        for (int i = 0; i < tokens.length; i++) {
            words[i] = new word(tokens[i]);
        }
        return new parsedLine(fields[0], words, Integer.parseInt(fields[2]));
    }
    
    public static class parsedLine{
        String headWord;
        word[] syntacticNgram;
        int totalCount;

        public parsedLine(String headWord, word[] syntacticNgram, int totalCount){
            this.headWord = headWord;
            this.syntacticNgram = syntacticNgram;
            this.totalCount = totalCount;
        }

        public String getHeadWord() {
            return headWord;
        }
        public word[] getSyntacticNgram() {
            return syntacticNgram;
        }
        public int getTotalCount() {
            return totalCount;
        }

    }

    public static class word{
        String word;
        String pos;
        String relation;
        String parentID;

        public word(String word, String pos, String relation, String parentID){
            this.word = word;
            this.pos = pos;
            this.relation = relation;
            this.parentID = parentID;
        }

        public word(String token){
            String[] parts = token.split("/");
            this.word = parts[0];
            this.pos = parts[1];
            this.relation = parts[2];
            this.parentID = parts[3];
        }

        public String getWord() {
            return word;
        }
        public String getPos() {
            return pos;
        }
        public String getRelation() {
            return relation;
        }
        public String getParentID() {
            return parentID;
        }
    }

    public static class PathExtraction {

        // A simple container to hold the output for the Mapper
        public static class ExtractionResult {
            public String path;
            public String slotX;
            public String slotY;
            public int totalCount;

            public ExtractionResult(String path, String slotX, String slotY, int count) {
                this.path = path;
                this.slotX = slotX;
                this.slotY = slotY;
                this.totalCount = count;
            }
        }

        // Inner class to help track slots before we finalize them
        private static class SlotCandidate {
            String word;
            String relationPart; // e.g. "nsubj:N" or "prep:with:pobj:N"
            boolean isSubject;   // Helps us decide which is X and which is Y

            public SlotCandidate(String word, String relationPart, boolean isSubject) {
                this.word = word;
                this.relationPart = relationPart;
                this.isSubject = isSubject;
            }
        }

        public static ExtractionResult processLine(String line) {
            // 1. Parse the line using your utils
            parsedLine parsed = parse(line);
            word[] words = parsed.getSyntacticNgram();

            // 2. Find the Root Node (The Verb)
            // In the format, the root has parentID == "0"
            int rootIndex = -1;
            for (int i = 0; i < words.length; i++) {
                if (words[i].getParentID().equals("0")) {
                    rootIndex = i;
                    break;
                }
            }

            if (rootIndex == -1) return null; // Should not happen in valid data
            word rootNode = words[rootIndex];

            // 3. Filter: Check Head Verb
            // Constraint: Head must be a Verb (VB*)
            if (!rootNode.getPos().startsWith("VB")) return null;

            // Constraint: Filter Auxiliary Verbs (is, are, have...)
            if (isStopVerb(rootNode.getWord())) return null;

            // 4. Find Slots
            // The Root's ID in the file is its array index + 1
            String rootID = String.valueOf(rootIndex + 1);
            List<SlotCandidate> candidates = new ArrayList<>();

            // Iterate through all words to find children of the Root
            for (int i = 0; i < words.length; i++) {
                word child = words[i];

                // Is this a direct child of the root?
                if (child.getParentID().equals(rootID)) {
                    
                    // CASE A: Direct Noun (e.g., nsubj, dobj)
                    if (child.getPos().startsWith("NN")) {
                        boolean isSubj = child.getRelation().contains("subj");
                        // Path Segment: "relation:N"
                        candidates.add(new SlotCandidate(child.getWord(), child.getRelation() + ":N", isSubj));
                    }
                    
                    // CASE B: Preposition Detour (e.g., "relies ON ...")
                    else if (child.getPos().equals("IN") || child.getPos().equals("TO")) {
                        // We need to look for the noun that is a child of THIS preposition
                        // The preposition's ID is its index + 1
                        String prepID = String.valueOf(i + 1);

                        // Search for the grandchild (noun inside preposition)
                        for (int j = 0; j < words.length; j++) {
                            word grandChild = words[j];
                            if (grandChild.getParentID().equals(prepID) && grandChild.getPos().startsWith("NN")) {
                                // Path Segment: "prep_rel:prep_word:pobj_rel:N"
                                // e.g. "prep:on:pobj:N"
                                String complexRel = child.getRelation() + ":" + child.getWord() + ":" + grandChild.getRelation() + ":N";
                                candidates.add(new SlotCandidate(grandChild.getWord(), complexRel, false));
                            }
                        }
                    }
                }
            }

            // 5. Binary Constraint: Must have exactly 2 noun slots
            if (candidates.size() != 2) return null;

            // 6. Assign Slots X and Y
            // Heuristic: If one is a subject, it becomes X. The other is Y.
            SlotCandidate slotX = candidates.get(0);
            SlotCandidate slotY = candidates.get(1);

            if (slotY.isSubject && !slotX.isSubject) {
                SlotCandidate temp = slotX;
                slotX = slotY;
                slotY = temp;
            }

            // 7. Stemming (Required by assignment)
            // Assumes you have a StemmerUtils class, as dsp3.utils doesn't have one.
            String stemX = stem(slotX.word); 
            String stemY = stem(slotY.word);

            // 8. Construct Path String
            // Format: SlotX_Rel : V : RootWord : V : SlotY_Rel
            StringBuilder pathBuilder = new StringBuilder();
            pathBuilder.append(slotX.relationPart)
                    .append(":V:").append(rootNode.getWord()).append(":V:")
                    .append(slotY.relationPart);

            return new ExtractionResult(pathBuilder.toString(), stemX, stemY, parsed.getTotalCount());
        }
    }
}
