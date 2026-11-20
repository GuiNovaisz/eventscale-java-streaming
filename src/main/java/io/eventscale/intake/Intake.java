package io.eventscale.intake;

import io.eventscale.events.Contracts;
import io.eventscale.events.Event;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import java.util.Map;
import java.util.UUID;

@Service
public class Intake {
    private final JdbcTemplate db;
    private final TransactionTemplate tx;
    private final Contracts contracts;
    public Intake(JdbcTemplate db, TransactionTemplate tx, Contracts contracts) { this.db=db; this.tx=tx; this.contracts=contracts; }
    public Map<String,Object> accept(String json) {
        Event e = contracts.decode(json);
        String content = contracts.canonical(e);
        return tx.execute(status -> {
            // Serialize concurrent requests for this aggregate before assigning the log sequence.
            db.queryForObject("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", Object.class, e.aggregateId());
            db.update("INSERT INTO intake(event_id, aggregate_id, content) VALUES (?,?,?) ON CONFLICT(event_id) DO NOTHING", e.eventId(), e.aggregateId(), content);
            var existing = db.queryForMap("SELECT content, published_at FROM intake WHERE event_id=?", e.eventId());
            if (!content.equals(existing.get("content"))) throw new Conflict();
            return Map.of("eventId", e.eventId(), "status", existing.get("published_at") == null ? "ACCEPTED" : "PUBLISHED");
        });
    }
    public Map<String,Object> status(UUID id) {
        var rows = db.queryForList("SELECT event_id, ingested_at, published_at, partition_id, kafka_offset FROM intake WHERE event_id=?", id);
        if (rows.isEmpty()) throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.NOT_FOUND);
        return rows.getFirst();
    }
    public static class Conflict extends RuntimeException {}
}
