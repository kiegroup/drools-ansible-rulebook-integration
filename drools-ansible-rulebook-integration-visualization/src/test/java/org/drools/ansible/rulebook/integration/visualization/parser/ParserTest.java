package org.drools.ansible.rulebook.integration.visualization.parser;

import org.drools.ansible.rulebook.integration.api.RuleFormat;
import org.drools.ansible.rulebook.integration.api.RuleNotation;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.base.prototype.PrototypeObjectType;
import org.drools.impact.analysis.graph.Graph;
import org.drools.impact.analysis.graph.Link;
import org.drools.impact.analysis.graph.ModelToGraphConverter;
import org.drools.impact.analysis.graph.Node;
import org.drools.impact.analysis.graph.ReactivityType;
import org.drools.impact.analysis.graph.graphviz.GraphImageGenerator;
import org.drools.impact.analysis.model.AnalysisModel;
import org.drools.impact.analysis.model.Rule;
import org.drools.impact.analysis.model.left.Constraint;
import org.drools.impact.analysis.model.left.Pattern;
import org.drools.impact.analysis.model.right.ConsequenceAction;
import org.drools.impact.analysis.model.right.DeleteSpecificFactAction;
import org.drools.impact.analysis.model.right.InsertAction;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@Disabled("Analyzer depends on Index.ConstraintType. Temporarily ignore this test.")
public class ParserTest {

    private String JSON =
            """
                    {
                      "name":"05 Post event",
                      "hosts":[
                        "all"
                      ],
                      "rules":[
                        {
                          "Rule":{
                            "name":"r1",
                            "condition":{
                              "AllCondition":[
                                {
                                  "EqualsExpression":{
                                    "lhs":{
                                      "Event":"i"
                                    },
                                    "rhs":{
                                      "Integer":1
                                    }
                                  }
                                }
                              ]
                            },
                            "actions":[
                              {
                                "Action":{
                                  "action":"post_event",
                                  "action_args":{
                                    "event":{
                                      "msg":"hello world"
                                    }
                                  }
                                }
                              }
                            ],
                            "enabled":true
                          }
                        },
                        {
                          "Rule":{
                            "name":"r2",
                            "condition":{
                              "AllCondition":[
                                {
                                  "EqualsExpression":{
                                    "lhs":{
                                      "Event":"msg"
                                    },
                                    "rhs":{
                                      "String":"hello world"
                                    }
                                  }
                                }
                              ]
                            },
                            "actions":[
                              {
                                "Action":{
                                  "action":"set_fact",
                                    "action_args": {
                                      "fact": {
                                        "status": "created"
                                      }
                                  }
                                }
                              }
                            ],
                            "enabled":true
                          }
                        },
                        {
                          "Rule":{
                            "name":"r3",
                            "condition":{
                              "AllCondition":[
                                {
                                  "EqualsExpression":{
                                    "lhs":{
                                      "Event":"status"
                                    },
                                    "rhs":{
                                      "String":"created"
                                    }
                                  }
                                }
                              ]
                            },
                            "actions":[
                              {
                                "Action":{
                                  "action":"debug",
                                  "action_args":{
                                  }
                                }
                              }
                            ],
                            "enabled":true
                          }
                        },
                        {
                          "Rule":{
                            "name":"r4",
                            "condition":{
                              "AllCondition":[
                                {
                                  "EqualsExpression":{
                                    "lhs":{
                                      "Event":"i"
                                    },
                                    "rhs":{
                                      "Integer":2
                                    }
                                  }
                                }
                              ]
                            },
                            "actions":[
                              {
                                "Action":{
                                  "action":"retract_fact",
                                  "action_args": {
                                    "fact": {
                                      "status": "created"
                                    }
                                  }
                                }
                              }
                            ],
                            "enabled":true
                          }
                        }
                      ]
                    }
                    """;

    @Test
    void parse4RulesWithPostEventSetFactRetractFact() {
        RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, JSON);
        AnalysisModel analysisModel = RulesSetParser.parse(rulesSet);

        List<Rule> rules = analysisModel.getPackages().get(0).getRules();
        assertThat(rules).hasSize(4);
        Rule rule1 = rules.get(0);
        Pattern pattern1 = rule1.getLhs().getPatterns().get(0);
        assertThat(pattern1.getPatternClass()).isEqualTo(PrototypeObjectType.class);
        Constraint constraint1 = pattern1.getConstraints().iterator().next();
        assertThat(constraint1.getProperty()).isEqualTo("i");
        assertThat(constraint1.getValue()).isEqualTo(1);
        ConsequenceAction action1 = rule1.getRhs().getActions().get(0);
        assertThat(action1.getClass()).isEqualTo(InsertAction.class);
        assertThat(((InsertAction)action1).getInsertedProperties()).hasSize(1);
        assertThat(((InsertAction)action1).getInsertedProperties().get(0).getProperty()).isEqualTo("msg");
        assertThat(((InsertAction)action1).getInsertedProperties().get(0).getValue()).isEqualTo("hello world");

        Rule rule2 = rules.get(1);
        Pattern pattern2 = rule2.getLhs().getPatterns().get(0);
        assertThat(pattern2.getPatternClass()).isEqualTo(PrototypeObjectType.class);
        Constraint constraint2 = pattern2.getConstraints().iterator().next();
        assertThat(constraint2.getProperty()).isEqualTo("msg");
        assertThat(constraint2.getValue()).isEqualTo("hello world");
        ConsequenceAction action2 = rule2.getRhs().getActions().get(0);
        assertThat(action2.getClass()).isEqualTo(InsertAction.class);
        assertThat(((InsertAction)action2).getInsertedProperties()).hasSize(1);
        assertThat(((InsertAction)action2).getInsertedProperties().get(0).getProperty()).isEqualTo("status");
        assertThat(((InsertAction)action2).getInsertedProperties().get(0).getValue()).isEqualTo("created");

        Rule rule3 = rules.get(2);
        Pattern pattern3 = rule3.getLhs().getPatterns().get(0);
        assertThat(pattern3.getPatternClass()).isEqualTo(PrototypeObjectType.class);
        Constraint constraint3 = pattern3.getConstraints().iterator().next();
        assertThat(constraint3.getProperty()).isEqualTo("status");
        assertThat(constraint3.getValue()).isEqualTo("created");
        assertThat(rule3.getRhs().getActions()).isEmpty(); // debug action is ignored

        Rule rule4 = rules.get(3);
        Pattern pattern4 = rule4.getLhs().getPatterns().get(0);
        assertThat(pattern4.getPatternClass()).isEqualTo(PrototypeObjectType.class);
        Constraint constraint4 = pattern4.getConstraints().iterator().next();
        assertThat(constraint4.getProperty()).isEqualTo("i");
        assertThat(constraint4.getValue()).isEqualTo(2);
        ConsequenceAction action4 = rule4.getRhs().getActions().get(0);
        assertThat(action4.getClass()).isEqualTo(DeleteSpecificFactAction.class);
        assertThat(((DeleteSpecificFactAction)action4).getSpecificProperties()).hasSize(1);
        assertThat(((DeleteSpecificFactAction)action4).getSpecificProperties().get(0).getProperty()).isEqualTo("status");
        assertThat(((DeleteSpecificFactAction)action4).getSpecificProperties().get(0).getValue()).isEqualTo("created");

        ModelToGraphConverter converter = new ModelToGraphConverter();
        Graph graph = converter.toGraph(analysisModel);

        assertLink(graph, "defaultpkg.r1", "defaultpkg.r2", ReactivityType.POSITIVE);
        assertLink(graph, "defaultpkg.r2", "defaultpkg.r3", ReactivityType.POSITIVE);
        assertLink(graph, "defaultpkg.r4", "defaultpkg.r3", ReactivityType.NEGATIVE);

        GraphImageGenerator generator = new GraphImageGenerator("ParserTest-graph");
        String filePath = generator.generateSvg(graph);
        assertThat(Path.of(filePath)).exists();
    }

    private void assertLink(Graph graph, String sourceFqdn, String targetFqdn, ReactivityType... expectedTypes) {
        Node source = graph.getNodeMap().get(sourceFqdn);
        Node target = graph.getNodeMap().get(targetFqdn);
        List<Link> outgoingLinks = source.getOutgoingLinks().stream().filter(l -> l.getTarget().equals(target)).collect(Collectors.toList());
        List<Link> incomingLinks = target.getIncomingLinks().stream().filter(l -> l.getSource().equals(source)).collect(Collectors.toList());
        assertThat(outgoingLinks).hasSameElementsAs(incomingLinks);

        List<ReactivityType> outgoingLinkTypelist = outgoingLinks.stream().map(l -> l.getReactivityType()).collect(Collectors.toList());
        List<ReactivityType> expectedTypeList = Arrays.asList(expectedTypes);
        assertThat(outgoingLinkTypelist).hasSameElementsAs(expectedTypeList);
    }
}
