package org.drools.ansible.rulebook.integration.api;

import org.drools.ansible.rulebook.integration.api.domain.temporal.TimeAmount;
import org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil;
import org.drools.base.facttemplates.Fact;
import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OnceAfterTest {

    @Test
    public void testOnceAfterWithOr() {
        String json =
                "{" +
                "    \"rules\": [\n" +
                "        {\n" +
                "            \"Rule\": {\n" +
                "                \"name\": \"r1\",\n" +
                "                \"condition\": {\n" +
                "                    \"AllCondition\": [\n" +
                "                        {\n" +
                "                            \"OrExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"EqualsExpression\": {\n" +
                "                                        \"lhs\": {\n" +
                "                                            \"Event\": \"alert.level\"\n" +
                "                                        },\n" +
                "                                        \"rhs\": {\n" +
                "                                            \"String\": \"warning\"\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"EqualsExpression\": {\n" +
                "                                        \"lhs\": {\n" +
                "                                            \"Event\": \"alert.level\"\n" +
                "                                        },\n" +
                "                                        \"rhs\": {\n" +
                "                                            \"String\": \"error\"\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            }\n" +
                "                        }\n" +
                "                    ]\n" +
                "                },\n" +
                "                \"action\": {\n" +
                "                    \"Action\": {\n" +
                "                        \"action\": \"print_event\",\n" +
                "                        \"action_args\": {}\n" +
                "                    }\n" +
                "                },\n" +
                "                \"enabled\": true,\n" +
                "                \"throttle\": {\n" +
                "                    \"group_by_attributes\": [\n" +
                "                        \"event.meta.hosts\",\n" +
                "                        \"event.alert.level\"\n" +
                "                    ],\n" +
                "                    \"once_after\": \"10 seconds\"\n" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"error\" } }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.advanceTime(3, TimeUnit.SECONDS).join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h2\" }, \"alert\": { \"level\":\"error\" } }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"warning\" } }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.advanceTime(4, TimeUnit.SECONDS).join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"error\" } }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.advanceTime(5, TimeUnit.SECONDS).join();
        assertEquals(1, matchedRules.size());
        assertEquals("r1", matchedRules.get(0).getRule().getName());

        for (int i = 0; i < 3; i++) {
            Fact fact = (Fact) matchedRules.get(0).getDeclarationValue("m_" + i);
            String host = fact.get("meta.hosts").toString();
            assertTrue( host.equals( "h1" ) || host.equals( "h2" ) );
            String level = fact.get("alert.level").toString();
            assertTrue( level.equals( "error" ) || level.equals( "warning" ) );

            Map map = (Map) fact.get(RulesModelUtil.ORIGINAL_MAP_FIELD);
            Map ruleEngineMeta = (Map) ((Map)map.get(RulesModelUtil.META_FIELD)).get(RulesModelUtil.RULE_ENGINE_META_FIELD);
            assertEquals( new TimeAmount(10, TimeUnit.SECONDS).toString(), ruleEngineMeta.get("once_after_time_window") );
            assertEquals( i == 0 ? 2 : 1, ruleEngineMeta.get("events_in_window") );
        }

        for (int i = 0; i < 2; i++) {
            matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"warning\" } }").join();
            assertEquals(0, matchedRules.size());

            matchedRules = rulesExecutor.advanceTime(11, TimeUnit.SECONDS).join();
            assertEquals(1, matchedRules.size());
            assertEquals("r1", matchedRules.get(0).getRule().getName());

            Fact fact = (Fact) matchedRules.get(0).getDeclarationValue("m");
            String host = fact.get("meta.hosts").toString();
            assertTrue(host.equals("h1"));
            String level = fact.get("alert.level").toString();
            assertTrue(level.equals("warning"));

            Map map = (Map) fact.get(RulesModelUtil.ORIGINAL_MAP_FIELD);
            Map ruleEngineMeta = (Map) ((Map)map.get(RulesModelUtil.META_FIELD)).get(RulesModelUtil.RULE_ENGINE_META_FIELD);
            assertEquals( new TimeAmount(10, TimeUnit.SECONDS).toString(), ruleEngineMeta.get("once_after_time_window") );
            assertEquals( 1, ruleEngineMeta.get("events_in_window") );
        }

        rulesExecutor.dispose();
    }
}
