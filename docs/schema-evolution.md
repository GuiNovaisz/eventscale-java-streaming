# Schema evolution

OrderCreated v1 remains readable after introducing v2's optional channel. Normalization supplies `unknown` for absent channel and preserves the original version/event identity. Current consumers dispatch versions explicitly. Both unit and real-broker integration tests exercise old v1 and new v2 records. Unsupported 999 is classified; it is never treated as v1.

JSON and explicit eventVersion keep the compatibility behavior visible for a small portfolio. Avro, Protobuf and Schema Registry would add deployment/governance complexity without a demonstrated need in this scope. No compatibility API/registry/full governance is claimed. Optional fields in known versions are tolerated; unknown versions are rejected, so universal forward compatibility is specifically absent.

Future incompatible payloads require a new supported contract, compatibility fixtures, an upcaster or new topic, and replay validation before activating consumers. Event time stays occurredAt. This milestone has no windowing: late valid facts contribute to cumulative analytics; late illegal lifecycle transitions enter state quarantine. Window/grace/watermark policy is therefore not applicable yet.
