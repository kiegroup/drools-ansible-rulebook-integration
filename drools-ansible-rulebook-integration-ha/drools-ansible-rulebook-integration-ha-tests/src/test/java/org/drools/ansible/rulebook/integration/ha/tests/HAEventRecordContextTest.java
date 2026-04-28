package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.List;

import org.drools.ansible.rulebook.integration.ha.api.HASessionContext;
import org.drools.ansible.rulebook.integration.ha.api.HAUtils;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.drools.ansible.rulebook.integration.ha.model.EventRecordChange;
import org.drools.ansible.rulebook.integration.ha.model.EventRecordEntry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HAEventRecordContextTest {

    @Test
    void tracksEventRecordDeltasWithStableSequences() {
        HASessionContext context = new HASessionContext();
        EventRecord first = new EventRecord("{\"i\":1}", 1_000L, EventRecord.RecordType.EVENT);
        EventRecord second = new EventRecord("{\"j\":2}", 1_000L, EventRecord.RecordType.FACT);

        context.addTrackedRecord("event-1", first, 10L);
        context.addTrackedRecord("fact-1", second, 11L);

        List<EventRecordChange> insertChanges = context.drainEventRecordChanges();

        assertThat(insertChanges).hasSize(2);
        assertThat(insertChanges).extracting(EventRecordChange::getType)
                .containsExactly(EventRecordChange.Type.UPSERT, EventRecordChange.Type.UPSERT);
        assertThat(insertChanges).extracting(EventRecordChange::getRecordIdentifier)
                .containsExactly("event-1", "fact-1");
        assertThat(insertChanges).extracting(change -> change.getEntry().getRecordSequence())
                .containsExactly(0L, 1L);
        assertThat(context.drainEventRecordChanges()).isEmpty();

        context.updateTrackedRecordByFactHandle(10L, "{\"i\":10}");

        List<EventRecordChange> updateChanges = context.drainEventRecordChanges();

        assertThat(updateChanges).hasSize(1);
        assertThat(updateChanges.get(0).getType()).isEqualTo(EventRecordChange.Type.UPSERT);
        assertThat(updateChanges.get(0).getRecordIdentifier()).isEqualTo("event-1");
        assertThat(updateChanges.get(0).getEntry().getRecordSequence()).isEqualTo(0L);
        assertThat(updateChanges.get(0).getEntry().getRecord().getEventJson()).isEqualTo("{\"i\":10}");

        context.removeTrackedRecordByFactHandle(10L);

        List<EventRecordChange> deleteChanges = context.drainEventRecordChanges();

        assertThat(deleteChanges).hasSize(1);
        assertThat(deleteChanges.get(0).getType()).isEqualTo(EventRecordChange.Type.DELETE);
        assertThat(deleteChanges.get(0).getRecordIdentifier()).isEqualTo("event-1");

        List<EventRecordEntry> snapshot = context.snapshotEventRecordEntries();

        assertThat(snapshot).hasSize(1);
        assertThat(snapshot.get(0).getRecordIdentifier()).isEqualTo("fact-1");
        assertThat(snapshot.get(0).getRecordSequence()).isEqualTo(1L);
    }

    @Test
    void eventRecordHashesAreDeterministicAndCanonicalBySequence() {
        EventRecordEntry first = new EventRecordEntry(
                "event-1",
                0L,
                new EventRecord("{\"i\":1}", 1_000L, EventRecord.RecordType.EVENT));
        EventRecordEntry second = new EventRecordEntry(
                "event-2",
                1L,
                new EventRecord("{\"j\":2}", 1_000L, EventRecord.RecordType.EVENT));

        assertThat(HAUtils.calculateEventRecordSHA(first))
                .isEqualTo(HAUtils.calculateEventRecordSHA(new EventRecordEntry(
                        "event-1",
                        0L,
                        new EventRecord("{\"i\":1}", 1_000L, EventRecord.RecordType.EVENT))));

        String manifest = HAUtils.calculateEventRecordsManifestSHA(List.of(first, second));
        String reversedManifest = HAUtils.calculateEventRecordsManifestSHA(List.of(second, first));
        String changedPayloadManifest = HAUtils.calculateEventRecordsManifestSHA(List.of(
                new EventRecordEntry("event-1", 0L, new EventRecord("{\"i\":10}", 1_000L, EventRecord.RecordType.EVENT)),
                second));

        assertThat(manifest).isEqualTo(reversedManifest);
        assertThat(manifest).isNotEqualTo(changedPayloadManifest);
    }
}
