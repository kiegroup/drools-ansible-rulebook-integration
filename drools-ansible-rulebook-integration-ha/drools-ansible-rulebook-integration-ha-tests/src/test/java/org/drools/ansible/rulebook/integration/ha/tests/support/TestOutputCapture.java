package org.drools.ansible.rulebook.integration.ha.tests.support;

import org.drools.ansible.rulebook.integration.ha.tests.support.TestOutputCapture;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

public final class TestOutputCapture {

    private static final Object LOCK = new Object();

    private TestOutputCapture() {
    }

    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }

    public static String captureStdout(ThrowingRunnable action) throws Exception {
        synchronized (LOCK) {
            PrintStream originalOut = System.out;
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            PrintStream capture = new PrintStream(buffer, true, StandardCharsets.UTF_8);
            try {
                System.setOut(capture);
                action.run();
            } finally {
                capture.flush();
                System.setOut(originalOut);
                capture.close();
            }
            return buffer.toString(StandardCharsets.UTF_8);
        }
    }
}
