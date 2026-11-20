package io.eventscale;
import io.eventscale.events.Contracts;
import io.eventscale.projection.Transitions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;

class ContractsTest {
    final Contracts contracts=new Contracts();
    @ParameterizedTest @ValueSource(ints={1,2}) void backwardCompatibleCreation(int version) {
        var e=contracts.decode(Samples.event(UUID.randomUUID(),"o1","order.created",version));
        assertThat(contracts.decode(contracts.normalize(e)).payload().path("channel").asText()).isEqualTo(version==1?"unknown":"web");
        assertThat(e.occurredAt().toString()).isEqualTo("2026-11-01T12:00:00Z");
    }
    @ParameterizedTest @ValueSource(strings={"payment.confirmed","order.cancelled","order.shipped","order.delivered"}) void knownContracts(String type) {
        assertThat(contracts.decode(Samples.event(UUID.randomUUID(),"o1",type,1)).eventType()).isEqualTo(type);
    }
    @Test void unknownVersion() { assertThatThrownBy(()->contracts.decode(Samples.event(UUID.randomUUID(),"o1","order.created",999))).hasMessage("UNSUPPORTED_VERSION"); }
    @Test void unknownType() { assertThatThrownBy(()->contracts.decode(Samples.event(UUID.randomUUID(),"o1","java.lang.Runtime",1))).hasMessage("UNKNOWN_TYPE"); }
    @Test void malformedJson() { assertThatThrownBy(()->contracts.decode("{bad")).hasMessage("MALFORMED_JSON"); }
    @Test void missingIdentity() { assertThatThrownBy(()->contracts.decode(Samples.created("o1").replaceFirst("\"eventId\":\"[^\"]+\"","\"eventId\":null"))).hasMessage("INVALID_ENVELOPE"); }
    @Test void moneyValidation() { assertThatThrownBy(()->contracts.decode(Samples.created("o1").replace("42.50","-1"))).hasMessage("INVALID_MONEY"); }
    @Test void keyValidation() { assertThatThrownBy(()->contracts.decode(Samples.created("o1").replace("\"orderId\":\"o1\"","\"orderId\":\"o2\""))).hasMessage("KEY_MISMATCH"); }
    @Test void boundedInput() { assertThatThrownBy(()->contracts.decode("x".repeat(Contracts.MAX_BYTES+1))).hasMessage("SIZE_LIMIT"); }
    @Test void canonicalObjectOrder() {
        String raw=Samples.created("o1");
        assertThat(contracts.canonical(contracts.decode(raw))).isEqualTo(contracts.canonical(contracts.decode(raw.replace("\"total\":42.50,\"currency\":\"BRL\"","\"currency\":\"BRL\",\"total\":42.50"))));
    }
    @Test void lifecycle() {
        String state=null;
        for(String type:new String[]{"order.created","payment.confirmed","order.shipped","order.delivered"}) state=Transitions.apply(state,type);
        assertThat(state).isEqualTo("DELIVERED");
    }
    @Test void inconsistentBusinessOrdering() { assertThatThrownBy(()->Transitions.apply("CREATED","order.shipped")).isInstanceOf(Transitions.InconsistentEvent.class); }
    @Test void cancellationTerminal() { assertThat(Transitions.apply("PAID","order.cancelled")).isEqualTo("CANCELLED"); assertThatThrownBy(()->Transitions.apply("CANCELLED","payment.confirmed")).isInstanceOf(Transitions.InconsistentEvent.class); }
    @Test void rejectsTimestampOutsideStorageRange() { assertThatThrownBy(()->contracts.decode(Samples.created("o1").replace("2026-11-01T12:00:00Z","+10000-01-01T00:00:00Z"))).hasMessage("INVALID_EVENT_TIME"); }
    @Test void rejectsNumericPayloadIdentity() { assertThatThrownBy(()->contracts.decode(Samples.created("123").replace("\"orderId\":\"123\"","\"orderId\":123"))).hasMessage("KEY_MISMATCH"); }
    @Test void rejectsNonStringOptionalChannel() { assertThatThrownBy(()->contracts.decode(Samples.event(UUID.randomUUID(),"o1","order.created",2).replace("\"channel\":\"web\"","\"channel\":42"))).hasMessage("INVALID_CHANNEL"); }
}
