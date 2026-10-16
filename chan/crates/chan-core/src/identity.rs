//! Participant identity resolution.

use crate::address::ParticipantId;

/// Resolve participant identity from explicit input and the environment.
pub fn resolve_participant(explicit: Option<&str>, default: &str) -> ParticipantId {
    if let Some(name) = explicit {
        return ParticipantId::new(name);
    }
    if let Ok(name) = std::env::var("CHAN_PARTICIPANT") {
        return ParticipantId::new(name);
    }
    if let Ok(ses) = std::env::var("OPENCODE_SESSION_ID") {
        return ParticipantId::new(format!("oc:{ses}"));
    }
    ParticipantId::new(default)
}

#[cfg(test)]
#[allow(unsafe_code)]
mod tests {
    use super::*;

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn resolve_participant_ordering() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev_cp = std::env::var("CHAN_PARTICIPANT").ok();
        let prev_ses = std::env::var("OPENCODE_SESSION_ID").ok();
        unsafe {
            std::env::remove_var("CHAN_PARTICIPANT");
            std::env::remove_var("OPENCODE_SESSION_ID");
        }

        assert_eq!(resolve_participant(None, "default").as_str(), "default");

        unsafe {
            std::env::set_var("OPENCODE_SESSION_ID", "ses123");
        }
        assert_eq!(resolve_participant(None, "default").as_str(), "oc:ses123");

        unsafe {
            std::env::set_var("CHAN_PARTICIPANT", "w1");
        }
        assert_eq!(resolve_participant(None, "default").as_str(), "w1");

        assert_eq!(
            resolve_participant(Some("explicit"), "default").as_str(),
            "explicit"
        );

        match prev_cp {
            Some(v) => unsafe { std::env::set_var("CHAN_PARTICIPANT", v) },
            None => unsafe { std::env::remove_var("CHAN_PARTICIPANT") },
        }
        match prev_ses {
            Some(v) => unsafe { std::env::set_var("OPENCODE_SESSION_ID", v) },
            None => unsafe { std::env::remove_var("OPENCODE_SESSION_ID") },
        }
    }
}
