package org.drools.ansible.rulebook.integration.main.utils;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class StringPrintStream extends PrintStream {

    public static final String LINE_SEP = System.getProperty("line.separator");
    PrintStream other;
    List<String> stringList = new ArrayList<>();

    public StringPrintStream(PrintStream ps) {
        super(ps);
        other = ps;
    }

    public void print(String s) {
        other.print(s);
        stringList.add(s);
    }

    public void println(String s) {
        other.println(s);
        stringList.add(s);
    }

    public void println(Object o) {
        other.println(o);
        stringList.add(o.toString());
    }

    public List<String> getStringList() {
        return stringList;
    }
}
