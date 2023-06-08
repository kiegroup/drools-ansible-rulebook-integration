package org.drools.ansible.rulebook.integration.main;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.rulesengine.SlowAutomaticPseudoClock;
import org.drools.ansible.rulebook.integration.main.Main.ExecuteResult;
import org.drools.ansible.rulebook.integration.main.utils.StringPrintStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SlownessTest {

    static PrintStream originalOut = System.out;

    static StringPrintStream stringPrintStream = new StringPrintStream(System.out);

    @BeforeClass
    static public void beforeClass() throws Exception {
        System.setOut(stringPrintStream);
    }

    @AfterClass
    static public void afterClass() {
        System.setOut(originalOut);
    }

    @Test
    public void testOnceAfter() {
        try {
            SlowAutomaticPseudoClock.enable(11000, 4000);
            ExecuteResult result = Main.execute("56_once_after.json");// it has "delay_warning_threshold":"2 seconds"

            // a warning is logged. e.g.
            // WARN org.drools.ansible.rulebook.integration.api.rulesengine.SessionStatsCollector - r1 is fired with a delay of 3016 ms
            assertThat(stringPrintStream.getStringList()).anyMatch(s -> s.contains("r1 is fired with a delay of"));

            List<Map> returnedMatches = result.getReturnedMatches();
            assertThat(returnedMatches).hasSize(1);
            assertThat(returnedMatches.get(0)).containsOnlyKeys("r1");
        } finally {
            SlowAutomaticPseudoClock.resetAndDisable();
        }
    }
}
