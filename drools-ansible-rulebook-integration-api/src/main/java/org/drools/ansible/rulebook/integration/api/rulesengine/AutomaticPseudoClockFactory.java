package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.concurrent.TimeUnit;

import org.kie.api.internal.utils.KieService;

public interface AutomaticPseudoClockFactory extends KieService  {

    AutomaticPseudoClock createAutomaticPseudoClock(AbstractRulesEvaluator rulesEvaluator, long amount, TimeUnit unit);

    class Holder {

        private static final AutomaticPseudoClockFactory INSTANCE = createInstance();

        private Holder() {
        }

        static AutomaticPseudoClockFactory createInstance() {
            AutomaticPseudoClockFactory factory = KieService.load(AutomaticPseudoClockFactory.class);
            if (factory == null) {
                return new DefaultAutomaticPseudoClockFactory();
            }
            return factory;
        }
    }

    static AutomaticPseudoClockFactory get() {
        return AutomaticPseudoClockFactory.Holder.INSTANCE;
    }

    class DefaultAutomaticPseudoClockFactory implements AutomaticPseudoClockFactory {

        @Override
        public AutomaticPseudoClock createAutomaticPseudoClock(AbstractRulesEvaluator rulesEvaluator, long amount, TimeUnit unit) {
            return new AutomaticPseudoClock(rulesEvaluator, amount, unit);
        }
    }
}
