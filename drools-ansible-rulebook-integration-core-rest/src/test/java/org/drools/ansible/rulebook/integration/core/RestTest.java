/*
 * Copyright 2020 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.drools.ansible.rulebook.integration.core;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

@QuarkusTest
public class RestTest {

    private static final String JSON_RULES_1 = """
            {
              "rules": [
                {"Rule": {
                  "name": "R1",
                  "condition": "sensu.data.i == 1",
                  "action": {
                    "assert_fact": {
                      "ruleset": "Test rules4",
                      "fact": {
                        "j": 1
                      }
                    }
                  }
                }},
                {"Rule": {
                  "name": "R2",
                  "condition": "sensu.data.i == 2",
                  "action": {
                    "run_playbook": [
                      {
                        "name": "hello_playbook.yml"
                      }
                    ]
                  }
                }},
                {"Rule": {
                  "name": "R3",
                  "condition": "sensu.data.i == 3",
                  "action": {
                    "retract_fact": {
                      "ruleset": "Test rules4",
                      "fact": {
                        "j": 3
                      }
                    }
                  }
                }},
                {"Rule": {
                  "name": "R4",
                  "condition": "j == 1",
                  "action": {
                    "post_event": {
                      "ruleset": "Test rules4",
                      "fact": {
                        "j": 4
                      }
                    }
                  }
                }}
              ]
            }
            """;

    @Test
    public void testProcess() {
        // return the id of the newly generated RulesExecutor
        long id = given()
                .body(JSON_RULES_1)
                .contentType(ContentType.JSON)
                .when()
                .post("/create-rules-executor").as(long.class);

//        [
//            {
//                "ruleName": "R1",
//                "facts": {
//                    "sensu": {
//                        "data": {
//                            "i": 1
//                        }
//                    }
//                }
//            }
//        ]

        given()
                .body( "{ \"sensu\": { \"data\": { \"i\":1 } } }" )
                .contentType(ContentType.JSON)
                .when()
                .post("/rules-executors/" + id + "/process")
                .then()
                .statusCode(200)
                .body("ruleName", hasItem("R1"),
                      "facts.sensu.data.i", hasItem(1));

//        [
//            {
//                "ruleName": "R4",
//                "facts": {
//                    "j": 1
//                }
//            }
//        ]

        given()
                .body( "{ \"j\":1 }" )
                .contentType(ContentType.JSON)
                .when()
                .post("/rules-executors/" + id + "/process")
                .then()
                .statusCode(200)
                .body("ruleName", hasItem("R4"),
                      "facts.j", hasItem(1));
//                .log().body();
    }

    @Test
    public void testExecute() {
        // return the id of the newly generated RulesExecutor
        long id = given()
                .body(JSON_RULES_1)
                .contentType(ContentType.JSON)
                .when()
                .post("/create-rules-executor").as(long.class); // returns the number of processed rules

        given()
                .body( "{ \"sensu\": { \"data\": { \"i\":1 } } }" )
                .contentType(ContentType.JSON)
                .when()
                .post("/rules-executors/" + id + "/execute")
                .then()
                .statusCode(200)
                .body(is("2")); // returns the number of executed rules
    }

    private static final String JSON_RULES_2 = """
            {
               "rules":[
                  {"Rule": {
                     "name":"R1",
                     "condition":"sensu.data.i == 1"
                  }},
                  {"Rule": {
                     "name":"R2",
                     "condition":{
                        "all":[
                           "sensu.data.i == 3",
                           "j == 2"
                        ]
                     }
                  }},
                  {"Rule": {
                     "name":"R3",
                     "condition":{
                        "any":[
                           {
                              "all":[
                                 "sensu.data.i == 3",
                                 "j == 2"
                              ]
                           },
                           {
                              "all":[
                                 "sensu.data.i == 4",
                                 "j == 3"
                              ]
                           }
                        ]
                     }
                  }}
               ]
            }
            """;

    @Test
    public void testProcessWithLogicalOperators() {
        // return the id of the newly generated RulesExecutor
        long id = given()
                .body(JSON_RULES_2)
                .contentType(ContentType.JSON)
                .when()
                .post("/create-rules-executor").as(long.class);

        given()
                .body( "{ \"facts\": [ { \"sensu\": { \"data\": { \"i\":3 } } }, { \"j\":3 } ] }" )
                .contentType(ContentType.JSON)
                .when()
                .post("/rules-executors/" + id + "/process")
                .then()
                .statusCode(200)
                .body(is("[]"));

        given()
                .body( "{ \"sensu\": { \"data\": { \"i\":4 } } }" )
                .contentType(ContentType.JSON)
                .when()
                .post("/rules-executors/" + id + "/process")
                .then()
                .statusCode(200)
                .body("ruleName", hasItem("R3"),
                        "facts.j", hasItem(3));
    }
}
