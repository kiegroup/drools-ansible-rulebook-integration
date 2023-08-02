package org.drools.ansible.rulebook.integration.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.junit.Test;

import static org.drools.ansible.rulebook.integration.api.ObjectMapperFactory.createMapper;
import static org.junit.Assert.assertEquals;

public class YamlTest {

    private static final String YAML1 = """
            rules:
            - Rule:
                condition: sensu.data.i == 1
                action:
                  assert_fact:
                    ruleset: Test rules4
                    fact:
                      j: 1
            - Rule:
                condition: sensu.data.i == 2
                action:
                  run_playbook:
                    - name: hello_playbook.yml
            - Rule:
                condition: sensu.data.i == 3
                action:
                  retract_fact:
                    ruleset: Test rules4
                    fact:
                      j: 3
            - Rule:
                condition: j == 1
                action:
                  post_event:
                    ruleset: Test rules4
                    fact:
                      j: 4
            """;


    @Test
    public void testReadSimpleYaml() throws JsonProcessingException {
        ObjectMapper mapper = createMapper(new YAMLFactory());
        RulesSet rulesSet = mapper.readValue(YAML1, RulesSet.class);
        System.out.println(rulesSet);
    }

    @Test
    public void testExecuteRules() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromYaml(YAML1);
        int executedRules = rulesExecutor.executeFacts( "{ \"sensu\": { \"data\": { \"i\":1 } } }" ).join();
        assertEquals( 2, executedRules );
    }

    private static final String YAML2 ="""
            hosts:
            - localhost
            name: Demo rules
            rules:
            - Rule:
                condition:
                  AllCondition:
                  - EqualsExpression:
                      lhs:
                        Event: payload.provisioningState
                      rhs:
                        String: Succeeded
                enabled: true
                name: send to slack3
            - Rule:
                condition:
                  AllCondition:
                  - EqualsExpression:
                      lhs:
                        Event: payload.provisioningState
                      rhs:
                        String: Deleted
                enabled: true
                name: send to slack4
            - Rule:
                condition:
                  AllCondition:
                  - NotEqualsExpression:
                      lhs:
                        Event: payload.eventType
                      rhs:
                        String: GET
                enabled: true
                name: send to slack5
            - Rule:
                condition:
                  AllCondition:
                  - NotEqualsExpression:
                      lhs:
                        Event: payload.text
                      rhs:
                        String: ''
                enabled: true
                name: send to slack6
            - Rule:
                condition:
                  AllCondition:
                  - NotEqualsExpression:
                      lhs:
                        Event: payload.text
                      rhs:
                        String: ''
                enabled: true
                name: assert fact
            - Rule:
                condition:
                  AllCondition:
                  - NotEqualsExpression:
                      lhs:
                        Event: payload.text
                      rhs:
                        String: ''
            """;

    @Test
    public void testReadYaml() throws JsonProcessingException {
        ObjectMapper mapper = createMapper(new YAMLFactory());
        RulesSet rulesSet = mapper.readValue(YAML2, RulesSet.class);
        System.out.println(rulesSet);
    }
}