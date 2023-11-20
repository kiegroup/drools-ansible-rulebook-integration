package org.drools.ansible.rulebook.integration.visualization.parser;

import java.util.ArrayList;
import java.util.List;

import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionParseUtil;
import org.drools.impact.analysis.model.AnalysisModel;
import org.drools.impact.analysis.model.Package;
import org.drools.impact.analysis.model.Rule;
import org.drools.impact.analysis.model.left.LeftHandSide;
import org.drools.impact.analysis.model.right.RightHandSide;
import org.drools.model.impl.RuleBuilder;

/**
 * Parse drools-ansible RulesSet to drools-impact-analysis AnalysisModel
 */
public class RulesSetParser {

    public static AnalysisModel parse(RulesSet rulesSet) {
        AnalysisModel analysisModel = new AnalysisModel();
        List<org.drools.impact.analysis.model.Rule> rules = new ArrayList<>();
        rulesSet.getRules().forEach(ruleContainer -> {
            rules.add(parseRule(ruleContainer.getRule()));
        });
        Package pkg = new Package(RuleBuilder.DEFAULT_PACKAGE, rules);
        analysisModel.addPackage(pkg);
        return analysisModel;
    }

    private static Rule parseRule(org.drools.ansible.rulebook.integration.api.domain.Rule ansibleRule) {
        Rule analysisRule = new Rule(RuleBuilder.DEFAULT_PACKAGE, ansibleRule.getName(), null);
        LhsParser.parse(ansibleRule.getCondition(), analysisRule.getLhs());
        RhsParser.parse(ansibleRule.getActions(), analysisRule.getRhs());
        return analysisRule;
    }
}
