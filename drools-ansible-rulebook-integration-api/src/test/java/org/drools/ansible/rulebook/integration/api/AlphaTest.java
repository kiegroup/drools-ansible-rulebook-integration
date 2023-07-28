package org.drools.ansible.rulebook.integration.api;

import org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory;
import org.drools.base.definitions.InternalKnowledgePackage;
import org.drools.base.facttemplates.FactTemplate;
import org.drools.base.facttemplates.FactTemplateObjectType;
import org.drools.base.rule.IndexableConstraint;
import org.drools.base.util.index.ConstraintTypeOperator;
import org.drools.core.reteoo.AlphaNode;
import org.drools.core.reteoo.EntryPointNode;
import org.drools.core.reteoo.ObjectSink;
import org.drools.core.reteoo.ObjectTypeNode;
import org.drools.core.reteoo.Rete;
import org.drools.kiesession.rulebase.InternalKnowledgeBase;
import org.drools.model.Prototype;
import org.drools.modelcompiler.facttemplate.FactFactory;
import org.junit.Test;
import org.kie.api.definition.rule.Rule;
import org.kie.api.runtime.rule.Match;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AlphaTest {

    @Test
    public void testEqualsWithFixedValue() {
        String json =
                """
                {
                    "rules": [
                            {
                                "Rule": {
                                    "condition": {
                                        "AllCondition": [
                                            {
                                                "EqualsExpression": {
                                                    "lhs": {
                                                        "Event": "j"
                                                    },
                                                    "rhs": {
                                                        "Integer": "3"
                                                    }
                                                }
                                            }
                                        ]
                                    },
                                    "enabled": true,
                                    "name": "R1"
                                }
                            }
                        ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        Rete rete = ((InternalKnowledgeBase) rulesExecutor.asKieSession().getKieBase()).getRete();
        assertConstraintType(rete, "R1", ConstraintTypeOperator.EQUAL);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"i\": 3 }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"i\": 3, \"j\": 2 }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"i\": 3, \"j\": 3 }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("R1", matchedRules.get(0).getRule().getName());

        rulesExecutor.dispose();
    }

    private void assertConstraintType(Rete rete, String ruleName, ConstraintTypeOperator expectedType) {
        boolean asserted = false;
        assertEquals("expecting only 1 pkg.", 1, rete.getRuleBase().getPackages().length);
        InternalKnowledgePackage ipkg = rete.getRuleBase().getPackages()[0];
        Prototype default_proto = PrototypeFactory.getPrototype(PrototypeFactory.DEFAULT_PROTOTYPE_NAME);
        FactTemplate factTemplate = FactFactory.prototypeToFactTemplate(default_proto, ipkg);
        EntryPointNode epn = rete.getEntryPointNodes().values().iterator().next();
        ObjectTypeNode otn = epn.getObjectTypeNodes().get(new FactTemplateObjectType(factTemplate));
        ObjectSink[] sinks = otn.getObjectSinkPropagator().getSinks();
        for (ObjectSink objectSink : sinks) {
            AlphaNode alphaNode = (AlphaNode) objectSink;
            assertEquals("expecting that one rule has one AlphaNode.", 1, alphaNode.getAssociatedRules().length);
            Rule rule = alphaNode.getAssociatedRules()[0];
            if (rule.getName().equals(ruleName)) {
                IndexableConstraint constraint = (IndexableConstraint) alphaNode.getConstraint();
                assertEquals(expectedType, constraint.getConstraintType());
                asserted = true;
            }
        }
        assertTrue(asserted);
    }

    @Test
    public void testEqualsOn2Fields() {
        String json =
                """
                {
                    "rules": [
                            {
                                "Rule": {
                                    "condition": {
                                        "AllCondition": [
                                            {
                                                "EqualsExpression": {
                                                    "lhs": {
                                                        "Event": "i"
                                                    },
                                                    "rhs": {
                                                        "Event": "j"
                                                    }
                                                }
                                            }
                                        ]
                                    },
                                    "enabled": true,
                                    "name": null
                                }
                            }
                        ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"i\": 3 }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"i\": 3, \"j\": 2 }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"i\": 3, \"j\": 3 }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r_0", matchedRules.get(0).getRule().getName());

        rulesExecutor.dispose();
    }

    @Test
    public void testGreaterOn2Fields() {
        String json =
                """
                {
                    "rules": [
                            {
                                "Rule": {
                                    "condition": {
                                        "AllCondition": [
                                            {
                                                "GreaterThanExpression": {
                                                    "lhs": {
                                                        "Event": "i"
                                                    },
                                                    "rhs": {
                                                        "Event": "j"
                                                    }
                                                }
                                            }
                                        ]
                                    },
                                    "enabled": true,
                                    "name": null
                                }
                            }
                        ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"i\": 3 }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"i\": 3, \"j\": 3 }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"i\": 3, \"j\": 2 }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r_0", matchedRules.get(0).getRule().getName());

        rulesExecutor.dispose();
    }

    @Test
    public void testListContainsField() {

        String JSON1 =
                """
                {
                    "rules": [
                            {
                                    "Rule": {
                                        "name": "contains_rule_int",
                                        "condition": {
                                            "AllCondition": [
                                                {
                                                    "ListContainsItemExpression": {
                                                        "lhs": {
                                                            "Event": "id_list"
                                                        },
                                                        "rhs": {
                                                            "Event": "i"
                                                        }
                                                    }
                                                }
                                            ]
                                        },
                                        "action": {
                                            "Action": {
                                                "action": "debug",
                                                "action_args": {}
                                            }
                                        },
                                        "enabled": true
                                    }
                                }
                        ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"id_list\" : [2,4] }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.processFacts( "{ \"id_list\" : [2,4], \"i\" : 3 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"id_list\" : [1,3,5], \"i\" : 3 }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "contains_rule_int", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( "{ \"id_list\" : 2, \"i\" : 3 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"id_list\" : 3, \"i\" : 3 }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "contains_rule_int", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }
}
