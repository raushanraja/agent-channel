//! Identity and addressing types.

use serde::{Deserialize, Serialize};

macro_rules! id_newtype {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Wrap a string as this identity.
            pub fn new(s: impl Into<String>) -> Self {
                Self(s.into())
            }

            /// Borrow the underlying string.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

id_newtype!(
    /// A participant: `orchestrator`, `worker-1`, or `oc:<sessionID>`.
    ParticipantId
);
id_newtype!(
    /// A pub/sub room name.
    RoomId
);

/// Where a message is addressed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum Address {
    /// Point-to-point to a participant.
    Direct(ParticipantId),
    /// Publish to a room; fanned out to its subscribers.
    Room(RoomId),
    /// Convenience broadcast to the implicit `all` room.
    Broadcast,
}

impl Address {
    /// The implicit broadcast room id.
    #[must_use]
    pub fn broadcast_room() -> RoomId {
        RoomId::new("all")
    }
}

impl From<ParticipantId> for Address {
    fn from(p: ParticipantId) -> Self {
        Address::Direct(p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_serializes_and_roundtrips() {
        let a = Address::Room(RoomId::new("build"));
        let s = serde_json::to_string(&a).unwrap();
        let back: Address = serde_json::from_str(&s).unwrap();
        assert_eq!(a, back);
        assert!(matches!(back, Address::Room(r) if r.as_str() == "build"));
    }

    #[test]
    fn direct_and_broadcast_roundtrip() {
        for a in [
            Address::Direct(ParticipantId::new("w1")),
            Address::Broadcast,
        ] {
            let back: Address = serde_json::from_str(&serde_json::to_string(&a).unwrap()).unwrap();
            assert_eq!(a, back);
        }
    }
}
