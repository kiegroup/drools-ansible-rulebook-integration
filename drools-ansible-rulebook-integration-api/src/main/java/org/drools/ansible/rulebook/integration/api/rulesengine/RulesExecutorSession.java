package org.drools.ansible.rulebook.integration.api.rulesengine;

import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.core.common.InternalFactHandle;
import org.drools.core.common.ReteEvaluator;
import org.drools.core.time.TimerService;
import org.drools.core.time.impl.PseudoClockScheduler;
import org.kie.api.prototype.PrototypeEventInstance;
import org.kie.api.prototype.PrototypeFactInstance;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.ObjectFilter;
import org.kie.api.runtime.rule.AgendaFilter;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.runtime.rule.Match;
import org.kie.api.time.SessionPseudoClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import static org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil.mapToFact;


public class RulesExecutorSession {

    private static final Logger log = LoggerFactory.getLogger(RulesExecutorSession.class);
    private static final String PURGE_CANCELLED_JOB_EVENT_COUNT_THRESHOLD_PROPERTY = "drools.purge.cancelled.job.event.count.threshold";
    private static final int DEFAULT_PURGE_CANCELLED_JOB_EVENT_COUNT_THRESHOLD = 10;
    private static final int PURGE_CANCELLED_JOB_EVENT_COUNT_THRESHOLD;

    static {
        String purgeThresholdEnvValue = System.getenv("DROOLS_PURGE_CANCELLED_JOB_EVENT_COUNT_THRESHOLD");
        if (purgeThresholdEnvValue != null && !purgeThresholdEnvValue.isEmpty()) {
            System.setProperty(PURGE_CANCELLED_JOB_EVENT_COUNT_THRESHOLD_PROPERTY, purgeThresholdEnvValue);
        }
        PURGE_CANCELLED_JOB_EVENT_COUNT_THRESHOLD = Integer.getInteger(PURGE_CANCELLED_JOB_EVENT_COUNT_THRESHOLD_PROPERTY,
                                                                       DEFAULT_PURGE_CANCELLED_JOB_EVENT_COUNT_THRESHOLD);
        if (PURGE_CANCELLED_JOB_EVENT_COUNT_THRESHOLD <= 0) {
            throw new IllegalArgumentException("Cancelled pseudo-clock job purge threshold must be > 0");
        }
        log.info("Cancelled pseudo-clock job purge threshold set to {}", PURGE_CANCELLED_JOB_EVENT_COUNT_THRESHOLD);
    }

    protected final RulesSet rulesSet;

    private final KieSession kieSession;

    private final RulesExecutionController rulesExecutionController;

    private final long id;

    private final SessionStatsCollector sessionStatsCollector;

    private final RulesSetEventStructure rulesSetEventStructure;
    private int purgeCancelledJobCounter = 0;

    public RulesExecutorSession(RulesSet rulesSet, KieSession kieSession, RulesExecutionController rulesExecutionController, long id) {
        this.rulesSet = rulesSet;
        this.kieSession = kieSession;
        this.rulesExecutionController = rulesExecutionController;
        this.id = id;
        this.sessionStatsCollector = new SessionStatsCollector(id);
        this.rulesSetEventStructure = new RulesSetEventStructure(rulesSet);

        this.sessionStatsCollector.registerBaseLevelMemory(); // initial used memory after kbase/ksession creation
        initClock();
    }

    private void initClock() {
        getPseudoClock().advanceTime(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    protected long getId() {
        return id;
    }

    String getRuleSetName() {
        return rulesSet.getName();
    }

    Collection<? extends Object> getObjects() {
        return kieSession.getObjects();
    }

    Collection<? extends Object> getObjects(ObjectFilter filter) {
        return kieSession.getObjects(filter);
    }

    SessionStatsCollector getSessionStatsCollector() {
        return sessionStatsCollector;
    }

    protected InternalFactHandle insert(Map<String, Object> factMap, boolean event) {
        PrototypeFactInstance fact = mapToFact(factMap, event);
        if (event) {
            ((PrototypeEventInstance) fact).withExpiration(rulesSet.getEventsTtl().getAmount(), rulesSet.getEventsTtl().getTimeUnit());
        }
        InternalFactHandle fh = (InternalFactHandle) kieSession.insert(fact);
        if (event) {
            sessionStatsCollector.registerProcessedEvent(this, fh);
        }
        return fh;
    }

    protected void delete(FactHandle fh) {
        kieSession.delete(fh);
        purgeCancelledJobsIfSupported(fh instanceof InternalFactHandle internalFactHandle ? internalFactHandle : null);
    }

    /*
     * This method is a temporal workaround to call purgeCancelledJob with reflection
     * to reduce the memory consumption by retained jobs
     * This can be removed when the upstream drools-core is fixed (currently it's not purged until 1000 canceled jobs exist)
     */
    void purgeCancelledJobsIfSupported(InternalFactHandle fh) {
        if (fh == null || !fh.isEvent()) {
            return;
        }
        purgeCancelledJobCounter++;
        if (purgeCancelledJobCounter < PURGE_CANCELLED_JOB_EVENT_COUNT_THRESHOLD) {
            return;
        }
        purgeCancelledJobCounter = 0;
        try {
            TimerService timerService = ((ReteEvaluator) kieSession).getTimerService();
            if (timerService instanceof PseudoClockScheduler scheduler) {
                Method purgeCancelledJob = PseudoClockScheduler.class.getDeclaredMethod("purgeCancelledJob");
                purgeCancelledJob.setAccessible(true);
                purgeCancelledJob.invoke(scheduler);
            }
        } catch (ReflectiveOperationException e) {
            log.warn("Unable to purge cancelled pseudo-clock jobs after deleting event {}", fh.getId(), e);
        }
    }

    List<InternalFactHandle> deleteAllMatchingFacts(Map<String, Object> toBeRetracted, boolean allowPartialMatch, String... keysToExclude) {
        BiPredicate<Map<String, Object>, Map<String, Object>> factsComparator = allowPartialMatch ?
                (wmFact, retract) -> wmFact.entrySet().containsAll(retract.entrySet()) :
                (wmFact, retract) -> areFactsEqual(wmFact, retract, keysToExclude);

        Collection<FactHandle> fhs = kieSession.getFactHandles(o -> o instanceof PrototypeFactInstance && factsComparator.test( ((PrototypeFactInstance) o).asMap(), toBeRetracted ) );
        return new ArrayList<>( fhs ).stream().peek( kieSession::delete ).map( InternalFactHandle.class::cast ).collect(Collectors.toList());
    }

    private static boolean areFactsEqual(Map<String, Object> wmFact, Map<String, Object> retract, String... keysToExclude) {
        Set<String> checkedKeys = new HashSet<>();
        for (Map.Entry<String, Object> wmFactValue : wmFact.entrySet()) {
            String wmFactKey = wmFactValue.getKey();
            if (!isKeyToBeIgnored(wmFactKey, keysToExclude) && !wmFactValue.getValue().equals(retract.get(wmFactKey))) {
                return false;
            }
            checkedKeys.add(wmFactKey);
        }
        return checkedKeys.size() == wmFact.size();
    }

    private static boolean isKeyToBeIgnored(String wmFactKey, String... keysToExclude) {
        if (keysToExclude != null && keysToExclude.length > 0) {
            for (String keyToExclude : keysToExclude) {
                if (wmFactKey.equals(keyToExclude) || wmFactKey.startsWith(keyToExclude + ".")) {
                    return true;
                }
            }
        }
        return false;
    }

    int fireAllRules() {
        return kieSession.fireAllRules();
    }

    int fireAllRules(AgendaFilter agendaFilter) {
        return kieSession.fireAllRules(agendaFilter);
    }

    SessionStats dispose() {
        SessionStats stats = getSessionStats(true);
        kieSession.dispose();
        return stats;
    }

    SessionStats getSessionStats(boolean disposing) {
        return sessionStatsCollector.generateStats(this, disposing);
    }

    public void registerMatch(Match match) {
        sessionStatsCollector.registerMatch(this, match);
    }

    public void registerMatchedEvents(Collection<FactHandle> events) {
        sessionStatsCollector.registerMatchedEvents(events);
    }

    public void registerAsyncResponse(byte[] bytes) {
        sessionStatsCollector.registerAsyncResponse(bytes);
    }

    int rulesCount() {
        return rulesSet.getEnabledRulesNumber();
    }

    int disabledRulesCount() {
        return rulesSet.getDisabledRulesNumber();
    }

    void advanceTime( long amount, TimeUnit unit ) {
        SessionPseudoClock clock = getPseudoClock();
        clock.advanceTime(amount, unit);
        sessionStatsCollector.registerClockAdvance(amount, unit);
    }

    SessionPseudoClock getPseudoClock() {
        return kieSession.getSessionClock();
    }

    void setExecuteActions(boolean executeActions) {
        rulesExecutionController.setExecuteActions(executeActions);
    }

    public boolean isMatchMultipleRules() {
        return rulesSet.isMatchMultipleRules();
    }

    public KieSession asKieSession() {
        return kieSession;
    }

    public RulesSetEventStructure getRulesSetEventStructure() {
        return rulesSetEventStructure;
    }
}
