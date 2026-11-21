package io.eventscale.api;

import io.eventscale.events.Contracts;
import io.eventscale.intake.Intake;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
public class EventController {
    private final Intake intake; private final JdbcTemplate db;
    public EventController(Intake intake, JdbcTemplate db) { this.intake=intake; this.db=db; }
    @PostMapping(value="/events",consumes=MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String,Object>> post(@RequestBody String json) { return ResponseEntity.accepted().body(intake.accept(json)); }
    @GetMapping("/events/{id}") public Map<String,Object> status(@PathVariable UUID id) { return intake.status(id); }
    @GetMapping("/orders/{id}") public Map<String,Object> order(@PathVariable String id) {
        var rows=db.queryForList("SELECT order_id,status,last_event_id,event_time,channel FROM order_state WHERE version=(SELECT version FROM active_projection) AND order_id=?",id);
        if(rows.isEmpty()) throw new ResponseStatusException(HttpStatus.NOT_FOUND); return rows.getFirst();
    }
    @GetMapping("/analytics") public Object analytics() { return db.queryForList("SELECT event_type,currency,event_count,revenue FROM analytics WHERE version=(SELECT version FROM active_projection) ORDER BY event_type,currency"); }
    @ExceptionHandler(Contracts.InvalidEvent.class) ResponseEntity<ProblemDetail> invalid(Contracts.InvalidEvent e) {
        return ResponseEntity.badRequest().body(ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,e.getMessage()));
    }
    @ExceptionHandler(Intake.Conflict.class) ResponseEntity<ProblemDetail> conflict() {
        return ResponseEntity.status(409).body(ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT,"eventId already has different content"));
    }
}
