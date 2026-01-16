package com.example.helpers;

public class Token {
    public final int idx;        // 1-based index
    public final String word;
    public final String pos;
    public final String dep;
    public final int head;       // 0 for ROOT

    public Token(int idx, String word, String pos, String dep, int head) {
        this.idx = idx;
        this.word = word;
        this.pos = pos;
        this.dep = dep;
        this.head = head;
    }
}
