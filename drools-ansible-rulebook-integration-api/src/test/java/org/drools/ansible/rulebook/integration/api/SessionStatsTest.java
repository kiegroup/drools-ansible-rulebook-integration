package org.drools.ansible.rulebook.integration.api;

import org.drools.ansible.rulebook.integration.api.rulesengine.SessionStats;
import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class SessionStatsTest {

    @Test
    public void testWithDisabledRule() {
        String json =
                "{\n" +
                "  \"rules\": [\n" +
                "    {\"Rule\": {\n" +
                "      \"name\": \"R1\",\n" +
                "      \"condition\": \"sensu.data.i == 1\",\n" +
                "      \"action\": {\n" +
                "        \"assert_fact\": {\n" +
                "          \"ruleset\": \"Test rules4\",\n" +
                "          \"fact\": {\n" +
                "            \"j\": 1\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }},\n" +
                "    {\"Rule\": {\n" +
                "      \"name\": \"R2\",\n" +
                "      \"condition\": \"sensu.data.i == 2\",\n" +
                "      \"action\": {\n" +
                "        \"run_playbook\": [\n" +
                "          {\n" +
                "            \"name\": \"hello_playbook.yml\"\n" +
                "          }\n" +
                "        ]\n" +
                "      }\n" +
                "    }},\n" +
                "    {\"Rule\": {\n" +
                "      \"name\": \"R3\",\n" +
                "      \"condition\": \"sensu.data.i == 3\",\n" +
                "      \"action\": {\n" +
                "        \"retract_fact\": {\n" +
                "          \"ruleset\": \"Test rules4\",\n" +
                "          \"fact\": {\n" +
                "            \"j\": 3\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }},\n" +
                "    {\"Rule\": {\n" +
                "      \"name\": \"R4\",\n" +
                "      \"enabled\": false,\n" +
                "      \"condition\": \"j == 1\",\n" +
                "      \"action\": {\n" +
                "        \"post_event\": {\n" +
                "          \"ruleset\": \"Test rules4\",\n" +
                "          \"fact\": {\n" +
                "            \"j\": 4\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }}\n" +
                "  ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":1 } } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( "{ \"j\":1 }" ).join();
        assertEquals( 0, matchedRules.size() );

        SessionStats stats = rulesExecutor.getSessionStats();
        assertEquals( 3, stats.getNumberOfRules() );
        assertEquals( 1, stats.getNumberOfDisabledRules() );
        assertEquals( 1, stats.getRulesTriggered() );
        assertEquals( 1, stats.getPermanentStorageSize() );
        assertEquals( "R1", stats.getLastRuleFired() );

        rulesExecutor.dispose();
    }
}
