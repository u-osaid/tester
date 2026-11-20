use core::fmt;
use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet};
use std::format;
use std::string::String;
use std::vec::Vec;

use pump_shared::{
    Action, BleOp, BleResult, Completion, CompletionError, DriverInfo, Engine, Event, Notification,
    OpId, Secret, SimDevice, Slot, Timestamp, Uuid,
};

/// The part of an engine the harness drives: the host loop of guideline 04.
///
/// Every `pump_shared::Engine` is one. A CGM engine is driven through
/// [`CgmHost`] (feature `cgm`), because `cgm_shared::CgmEngine` is a separate
/// trait until the generic contract is split out of `pump-shared` (Open: Q5).
pub trait HostDriven {
    type Event;

    fn poll_action(&mut self) -> Option<Action>;

    fn poll_event(&mut self) -> Option<Self::Event>;

    fn handle_completion(
        &mut self,
        id: OpId,
        completion: Completion,
    ) -> Result<(), CompletionError>;

    fn handle_timeout(&mut self, now: Timestamp);

    fn poll_timeout(&self) -> Option<Timestamp>;

    fn handle_notification(&mut self, notification: Notification);
}

impl<T: Engine + ?Sized> HostDriven for T {
    type Event = Event;

    fn poll_action(&mut self) -> Option<Action> {
        Engine::poll_action(self)
    }

    fn poll_event(&mut self) -> Option<Event> {
        Engine::poll_event(self)
    }

    fn handle_completion(
        &mut self,
        id: OpId,
        completion: Completion,
    ) -> Result<(), CompletionError> {
        Engine::handle_completion(self, id, completion)
    }

    fn handle_timeout(&mut self, now: Timestamp) {
        Engine::handle_timeout(self, now);
    }

    fn poll_timeout(&self) -> Option<Timestamp> {
        Engine::poll_timeout(self)
    }

    fn handle_notification(&mut self, notification: Notification) {
        Engine::handle_notification(self, notification);
    }
}

/// Drives a `cgm_shared::CgmEngine` through the harness.
#[cfg(feature = "cgm")]
pub struct CgmHost<'a, E: cgm_shared::CgmEngine + ?Sized>(pub &'a mut E);

#[cfg(feature = "cgm")]
impl<E: cgm_shared::CgmEngine + ?Sized> HostDriven for CgmHost<'_, E> {
    type Event = cgm_shared::CgmEvent;

    fn poll_action(&mut self) -> Option<Action> {
        self.0.poll_action()
    }

    fn poll_event(&mut self) -> Option<cgm_shared::CgmEvent> {
        self.0.poll_event()
    }

    fn handle_completion(
        &mut self,
        id: OpId,
        completion: Completion,
    ) -> Result<(), CompletionError> {
        self.0.handle_completion(id, completion)
    }

    fn handle_timeout(&mut self, now: Timestamp) {
        self.0.handle_timeout(now);
    }

    fn poll_timeout(&self) -> Option<Timestamp> {
        self.0.poll_timeout()
    }

    fn handle_notification(&mut self, notification: Notification) {
        self.0.handle_notification(notification);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Fault {
    BleTimeout,
    BleDisconnected,
    BleStatus(u8),
    HostFails,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Violation {
    ResentAfterTimeout(Vec<u8>),
    CompletionRejected(CompletionError),
    NeverSettled,
    UnannouncedSubscribe(Uuid, Uuid),
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResentAfterTimeout(data) => write!(f, "write {data:02x?} resent after a timeout"),
            Self::CompletionRejected(error) => write!(f, "completion rejected: {error}"),
            Self::NeverSettled => f.write_str("engine never settled"),
            Self::UnannouncedSubscribe(service, characteristic) => {
                write!(f, "subscribed to unannounced {service}/{characteristic}")
            }
        }
    }
}

impl std::error::Error for Violation {}

#[derive(Debug)]
pub struct Harness<D, E = Event> {
    pub device: D,
    pub store: BTreeMap<Slot, Vec<u8>>,
    pub now: Timestamp,
    pub events: Vec<E>,
    pub transcript: Vec<String>,
    pub faults: BTreeMap<usize, Fault>,
    pub write_faults: BTreeMap<Uuid, Fault>,
    pub announced: Option<&'static DriverInfo>,
    pub resendable: BTreeSet<Uuid>,
    pub random: fn(usize) -> Vec<u8>,
    subscribed: BTreeSet<(Uuid, Uuid)>,
    actions: usize,
    timed_out_write: Option<(Uuid, Vec<u8>)>,
}

const MAX_STEPS: usize = 100_000;

fn counting(len: usize) -> Vec<u8> {
    (0..len).map(|i| i as u8).collect()
}

impl<D: SimDevice, E> Harness<D, E> {
    pub fn new(device: D) -> Self {
        Self {
            device,
            store: BTreeMap::new(),
            now: Timestamp::default(),
            events: Vec::new(),
            transcript: Vec::new(),
            faults: BTreeMap::new(),
            write_faults: BTreeMap::new(),
            announced: None,
            resendable: BTreeSet::new(),
            random: counting,
            subscribed: BTreeSet::new(),
            actions: 0,
            timed_out_write: None,
        }
    }

    pub fn run<H: HostDriven<Event = E> + ?Sized>(
        &mut self,
        engine: &mut H,
    ) -> Result<(), Violation> {
        for _ in 0..MAX_STEPS {
            self.deliver_notifications(engine);
            self.events
                .extend(core::iter::from_fn(|| engine.poll_event()));
            let Some(action) = engine.poll_action() else {
                return Ok(());
            };
            self.transcript.push(format!("{action:?}"));
            let id = action.id();
            let completion = self.perform(action)?;
            engine
                .handle_completion(id, completion)
                .map_err(Violation::CompletionRejected)?;
        }
        Err(Violation::NeverSettled)
    }

    pub fn restart(&mut self) {
        self.events.clear();
        self.faults.clear();
        self.write_faults.clear();
        self.subscribed.clear();
        self.timed_out_write = None;
    }

    pub fn advance<H: HostDriven<Event = E> + ?Sized>(
        &mut self,
        engine: &mut H,
        by: Duration,
    ) -> Result<(), Violation> {
        let end = Timestamp::from_millis(self.now.as_millis() + by.as_millis() as u64);
        loop {
            self.run(engine)?;
            let Some(at) = engine.poll_timeout().filter(|at| *at <= end) else {
                break;
            };
            self.now = self.now.max(at);
            engine.handle_timeout(self.now);
        }
        self.now = end;
        Ok(())
    }

    fn deliver_notifications<H: HostDriven<Event = E> + ?Sized>(&mut self, engine: &mut H) {
        while let Some(notification) = self.device.poll_notification(self.now) {
            let key = (notification.service, notification.characteristic);
            if self.subscribed.contains(&key) {
                self.transcript.push(format!("{notification:?}"));
                engine.handle_notification(notification);
            }
        }
    }

    fn perform(&mut self, action: Action) -> Result<Completion, Violation> {
        self.actions += 1;
        let fault = match &action {
            Action::Ble {
                op: BleOp::Write { characteristic, .. },
                ..
            } => self.write_faults.remove(characteristic),
            _ => None,
        }
        .or_else(|| self.faults.get(&self.actions).copied());
        if fault == Some(Fault::HostFails) && !matches!(action, Action::Ble { .. }) {
            return Ok(Completion::Failed);
        }
        Ok(match action {
            Action::Ble { op, .. } => Completion::Ble(self.ble(op, fault)?),
            Action::Store { slot, bytes, .. } => {
                self.store.insert(slot, bytes);
                Completion::Stored
            }
            Action::Load { slot, .. } => Completion::Loaded(self.store.get(&slot).cloned()),
            Action::Erase { slot, .. } => {
                self.store.remove(&slot);
                Completion::Stored
            }
            Action::Wrap { plaintext, .. } => Completion::Wrapped(plaintext.expose().to_vec()),
            Action::Unwrap { wrapped, .. } => Completion::Secret(Secret::new(wrapped)),
            Action::Random { len, .. } => Completion::Secret(Secret::new((self.random)(len))),
            Action::KeyAgree {
                key, peer_public, ..
            } => match self.device.key_agree(&key, &peer_public) {
                Some(shared) => Completion::Secret(Secret::new(shared.to_vec())),
                None => Completion::Failed,
            },
        })
    }

    fn ble(&mut self, op: BleOp, fault: Option<Fault>) -> Result<BleResult, Violation> {
        let written = match &op {
            BleOp::Write {
                characteristic,
                data,
                ..
            } if !self.resendable.contains(characteristic) => Some((*characteristic, data.clone())),
            _ => None,
        };
        if let Some((characteristic, data)) = &written {
            match &self.timed_out_write {
                Some(timed_out) if timed_out == &(*characteristic, data.clone()) => {
                    return Err(Violation::ResentAfterTimeout(data.clone()));
                }
                Some((timed_out, _)) if timed_out == characteristic => self.timed_out_write = None,
                _ => {}
            }
        }
        if let BleOp::Subscribe {
            service,
            characteristic,
            kind,
        } = op
        {
            if self
                .announced
                .is_some_and(|info| info.listens(service, characteristic) != Some(kind))
            {
                return Err(Violation::UnannouncedSubscribe(service, characteristic));
            }
        }
        let result = match fault {
            Some(Fault::BleTimeout) => BleResult::Timeout,
            Some(Fault::BleDisconnected) => BleResult::Disconnected,
            Some(Fault::BleStatus(code)) => BleResult::Status(code),
            _ => self.device.handle(&op, self.now),
        };
        if result == BleResult::Timeout {
            self.timed_out_write = written;
        }
        if result == BleResult::Done {
            match op {
                BleOp::Subscribe {
                    service,
                    characteristic,
                    ..
                } => {
                    self.subscribed.insert((service, characteristic));
                }
                BleOp::Unsubscribe {
                    service,
                    characteristic,
                } => {
                    self.subscribed.remove(&(service, characteristic));
                }
                _ => {}
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pump_shared::{KeyRef, Notification};
    use std::collections::VecDeque;
    use std::vec;

    use pump_shared::{
        BleOp, Class, Command, CommandId, EngineState, GattChar, Kind, NotifyKind, OpId,
        SubmitError, TherapyMachine, TherapyState, UtcTime, Variant,
    };

    #[derive(Default)]
    struct Scripted {
        actions: VecDeque<Action>,
        events: VecDeque<Event>,
        timeouts: VecDeque<Timestamp>,
        completions: Vec<String>,
        woken: Vec<Timestamp>,
        notified: Vec<Notification>,
        reject: bool,
        endless: bool,
        therapy: TherapyState,
    }

    impl TherapyMachine for Scripted {
        fn therapy(&self) -> &TherapyState {
            &self.therapy
        }

        fn therapy_mut(&mut self) -> &mut TherapyState {
            &mut self.therapy
        }
    }

    impl Engine for Scripted {
        fn submit(&mut self, _command: Command) -> Result<CommandId, SubmitError> {
            Ok(CommandId::new(1))
        }

        fn stop(&mut self) -> CommandId {
            CommandId::new(2)
        }

        fn handle_completion(
            &mut self,
            _id: OpId,
            completion: Completion,
        ) -> Result<(), CompletionError> {
            if self.reject {
                return Err(CompletionError::NotPending);
            }
            self.completions.push(format!("{completion:?}"));
            Ok(())
        }

        fn handle_timeout(&mut self, now: Timestamp) {
            self.timeouts.pop_front();
            self.woken.push(now);
        }

        fn handle_wall_clock(&mut self, _now: UtcTime) {}

        fn handle_link_lost(&mut self) {}

        fn handle_notification(&mut self, notification: Notification) {
            self.notified.push(notification);
        }

        fn poll_action(&mut self) -> Option<Action> {
            if self.endless {
                return Some(random(1));
            }
            self.actions.pop_front()
        }

        fn poll_event(&mut self) -> Option<Event> {
            self.events.pop_front()
        }

        fn poll_timeout(&self) -> Option<Timestamp> {
            self.timeouts.front().copied()
        }

        fn state(&self) -> EngineState {
            EngineState::Paired
        }
    }

    #[derive(Default)]
    struct Pump {
        agrees_keys: bool,
        pending: VecDeque<Notification>,
    }

    impl SimDevice for Pump {
        fn handle(&mut self, op: &BleOp, _now: Timestamp) -> BleResult {
            match op {
                BleOp::Read { .. } => BleResult::Data(vec![0xAA]),
                _ => BleResult::Done,
            }
        }

        fn key_agree(&mut self, key: &KeyRef, peer_public: &[u8; 32]) -> Option<[u8; 32]> {
            if self.agrees_keys {
                return Some(*peer_public);
            }
            SimDevice::key_agree(&mut NoKeys, key, peer_public)
        }

        fn poll_notification(&mut self, _now: Timestamp) -> Option<Notification> {
            self.pending.pop_front()
        }
    }

    struct NoKeys;

    impl SimDevice for NoKeys {
        fn handle(&mut self, _op: &BleOp, _now: Timestamp) -> BleResult {
            BleResult::Failed
        }
    }

    const ID: OpId = OpId::new(1);
    const UUID: Uuid = Uuid::from_bytes([0; 16]);

    fn random(len: usize) -> Action {
        Action::Random { id: ID, len }
    }

    fn read() -> Action {
        Action::Ble {
            id: ID,
            op: BleOp::Read {
                service: UUID,
                characteristic: UUID,
            },
        }
    }

    fn write(data: Vec<u8>) -> Action {
        Action::Ble {
            id: ID,
            op: BleOp::Write {
                service: UUID,
                characteristic: UUID,
                data,
            },
        }
    }

    fn key_agree() -> Action {
        Action::KeyAgree {
            id: ID,
            key: KeyRef::new("server"),
            peer_public: [7; 32],
        }
    }

    fn engine(actions: Vec<Action>) -> Scripted {
        Scripted {
            actions: actions.into(),
            ..Scripted::default()
        }
    }

    #[test]
    fn performs_every_host_action() {
        let mut engine = engine(vec![
            read(),
            Action::Store {
                id: ID,
                slot: Slot::Counters,
                class: Class::Monotonic,
                bytes: vec![1],
            },
            Action::Load {
                id: ID,
                slot: Slot::Counters,
            },
            Action::Erase {
                id: ID,
                slot: Slot::Counters,
            },
            Action::Load {
                id: ID,
                slot: Slot::Counters,
            },
            Action::Wrap {
                id: ID,
                plaintext: Secret::new(vec![2]),
            },
            Action::Unwrap {
                id: ID,
                wrapped: vec![3],
            },
            random(2),
            key_agree(),
        ]);
        let mut harness = Harness::new(Pump::default());
        harness.run(&mut engine).unwrap();
        assert_eq!(
            engine.completions,
            [
                "Ble(Data([170]))",
                "Stored",
                "Loaded(Some([1]))",
                "Stored",
                "Loaded(None)",
                "Wrapped([2])",
                "Secret(Secret(1 bytes))",
                "Secret(Secret(2 bytes))",
                "Failed",
            ]
        );
        assert!(harness.store.is_empty());
        assert_eq!(harness.transcript.len(), 9);
    }

    #[test]
    fn draws_random_bytes_from_the_given_source() {
        let mut engine = engine(vec![random(3)]);
        let mut harness = Harness::new(Pump::default());
        harness.random = |len| vec![9; len];
        harness.run(&mut engine).unwrap();
        assert_eq!(engine.completions, ["Secret(Secret(3 bytes))"]);
    }

    #[test]
    fn violations_explain_themselves() {
        let messages = [
            Violation::ResentAfterTimeout(vec![0xAB]),
            Violation::CompletionRejected(CompletionError::NotPending),
            Violation::NeverSettled,
            Violation::UnannouncedSubscribe(UUID, PROGRESS),
        ]
        .map(|violation| violation.to_string());
        assert_eq!(
            messages,
            [
                "write [ab] resent after a timeout",
                "completion rejected: no pending operation with this id",
                "engine never settled",
                "subscribed to unannounced 00000000-0000-0000-0000-000000000000/02020202-0202-0202-0202-020202020202",
            ]
        );
    }

    #[test]
    fn simulator_can_agree_keys() {
        let mut engine = engine(vec![key_agree()]);
        Harness::new(Pump {
            agrees_keys: true,
            ..Pump::default()
        })
        .run(&mut engine)
        .unwrap();
        assert_eq!(engine.completions, ["Secret(Secret(32 bytes))"]);
    }

    #[test]
    fn injects_faults_by_action_number() {
        let mut engine = engine(vec![read(), read(), read(), random(1), read()]);
        let mut harness = Harness::new(Pump::default());
        harness.faults = BTreeMap::from([
            (1, Fault::BleTimeout),
            (2, Fault::BleDisconnected),
            (3, Fault::BleStatus(0x8B)),
            (4, Fault::HostFails),
            (5, Fault::HostFails),
        ]);
        harness.run(&mut engine).unwrap();
        assert_eq!(
            engine.completions,
            [
                "Ble(Timeout)",
                "Ble(Disconnected)",
                "Ble(Status(139))",
                "Failed",
                "Ble(Data([170]))"
            ]
        );
    }

    #[test]
    fn injects_a_fault_into_the_first_write_to_a_characteristic() {
        let mut engine = engine(vec![read(), write(vec![1]), write(vec![2])]);
        let mut harness = Harness::new(Pump::default());
        harness.write_faults.insert(UUID, Fault::BleStatus(0x87));
        harness.run(&mut engine).unwrap();
        assert_eq!(
            engine.completions,
            ["Ble(Data([170]))", "Ble(Status(135))", "Ble(Done)"]
        );
        assert!(harness.write_faults.is_empty());
    }

    #[test]
    fn flags_a_write_resent_after_timeout() {
        let mut engine = engine(vec![write(vec![9]), write(vec![9])]);
        let mut harness = Harness::new(Pump::default());
        harness.faults.insert(1, Fault::BleTimeout);
        assert_eq!(
            harness.run(&mut engine),
            Err(Violation::ResentAfterTimeout(vec![9]))
        );
    }

    #[test]
    fn a_restart_forgets_the_previous_engine() {
        let mut engine = engine(vec![write(vec![9])]);
        let mut harness = Harness::new(Pump::default());
        harness.faults.insert(1, Fault::BleTimeout);
        harness.write_faults.insert(UUID, Fault::BleTimeout);
        harness.run(&mut engine).unwrap();
        harness.faults.insert(9, Fault::BleTimeout);
        harness.restart();
        assert!(harness.events.is_empty() && harness.faults.is_empty());
        assert!(harness.write_faults.is_empty());
        let mut engine = self::engine(vec![write(vec![9])]);
        assert_eq!(harness.run(&mut engine), Ok(()));
    }

    #[test]
    fn allows_a_new_write_after_timeout() {
        let mut engine = engine(vec![write(vec![9]), write(vec![10])]);
        let mut harness = Harness::new(Pump::default());
        harness.faults.insert(1, Fault::BleTimeout);
        assert_eq!(harness.run(&mut engine), Ok(()));
    }

    #[test]
    fn flags_a_rejected_completion() {
        let mut engine = Scripted {
            reject: true,
            ..engine(vec![random(1)])
        };
        assert_eq!(
            Harness::new(Pump::default()).run(&mut engine),
            Err(Violation::CompletionRejected(CompletionError::NotPending))
        );
    }

    #[test]
    fn flags_an_engine_that_never_settles() {
        let mut engine = Scripted {
            endless: true,
            ..Scripted::default()
        };
        assert_eq!(
            Harness::new(Pump::default()).run(&mut engine),
            Err(Violation::NeverSettled)
        );
    }

    #[test]
    fn advances_virtual_time_through_due_timeouts() {
        let mut engine = Scripted {
            timeouts: [Timestamp::from_millis(100), Timestamp::from_millis(5_000)].into(),
            events: [Event::StateChanged(EngineState::Paired)].into(),
            ..Scripted::default()
        };
        let mut harness = Harness::new(Pump::default());
        harness
            .advance(&mut engine, Duration::from_secs(1))
            .unwrap();
        assert_eq!(engine.woken, [Timestamp::from_millis(100)]);
        assert_eq!(harness.now, Timestamp::from_millis(1_000));
        assert_eq!(harness.events, [Event::StateChanged(EngineState::Paired)]);
    }

    #[test]
    fn advance_reports_violations() {
        let mut engine = Scripted {
            reject: true,
            ..engine(vec![random(1)])
        };
        let result = Harness::new(Pump::default()).advance(&mut engine, Duration::from_secs(1));
        assert_eq!(
            result,
            Err(Violation::CompletionRejected(CompletionError::NotPending))
        );
    }

    #[test]
    fn scripted_engine_answers_the_rest_of_the_contract() {
        let mut engine = Scripted::default();
        assert_eq!(engine.submit(Command::Status), Ok(CommandId::new(1)));
        assert_eq!(engine.stop(), CommandId::new(2));
        engine.handle_wall_clock(UtcTime::from_unix_millis(0));
        engine.handle_link_lost();
        assert_eq!(engine.state(), EngineState::Paired);
        assert_eq!(engine.therapy().mode, pump_shared::PumpMode::Unknown);
        assert_eq!(engine.therapy_mut().mode, pump_shared::PumpMode::Unknown);
        assert_eq!(
            NoKeys.handle(&BleOp::Disconnect, Timestamp::default()),
            BleResult::Failed
        );
        assert_eq!(NoKeys.poll_notification(Timestamp::default()), None);
    }

    const PROGRESS: Uuid = Uuid::from_bytes([2; 16]);

    static INFO: DriverInfo = DriverInfo {
        id: "sim",
        name: "Sim",
        kind: Kind::Pump,
        version: "0",
        api_version: pump_shared::API_VERSION,
        protocol: "sim",
        ble: pump_shared::BleMatch {
            name_prefixes: &[],
            service_uuids: &[],
            manufacturer_id: None,
        },
        gatt: &[GattChar {
            name: "progress",
            service: UUID,
            characteristic: PROGRESS,
            notify: Some(NotifyKind::Notification),
        }],
        variants: &[Variant {
            id: "sim",
            name: "Sim",
        }],
        commands: &[],
        features: &[],
        slots: &[],
    };

    fn subscribe(characteristic: Uuid, kind: NotifyKind) -> Action {
        Action::Ble {
            id: ID,
            op: BleOp::Subscribe {
                service: UUID,
                characteristic,
                kind,
            },
        }
    }

    fn progress(byte: u8) -> Notification {
        Notification {
            service: UUID,
            characteristic: PROGRESS,
            data: vec![byte],
        }
    }

    fn pump_notifying(bytes: &[u8]) -> Pump {
        Pump {
            pending: bytes.iter().map(|b| progress(*b)).collect(),
            ..Pump::default()
        }
    }

    #[test]
    fn delivers_notifications_only_while_subscribed() {
        let mut harness = Harness::new(pump_notifying(&[1]));
        let mut scripted = engine(vec![]);
        harness.run(&mut scripted).unwrap();
        assert!(scripted.notified.is_empty());

        let mut harness = Harness::new(pump_notifying(&[]));
        let mut scripted = engine(vec![
            subscribe(PROGRESS, NotifyKind::Notification),
            random(1),
        ]);
        harness.run(&mut scripted).unwrap();
        harness.device.pending.push_back(progress(2));
        scripted.actions.push_back(Action::Ble {
            id: ID,
            op: BleOp::Unsubscribe {
                service: UUID,
                characteristic: PROGRESS,
            },
        });
        scripted.actions.push_back(random(1));
        harness.run(&mut scripted).unwrap();
        harness.device.pending.push_back(progress(3));
        harness.run(&mut scripted).unwrap();
        assert_eq!(scripted.notified, [progress(2)]);
    }

    #[test]
    fn a_failed_subscribe_does_not_subscribe() {
        let mut harness = Harness::new(pump_notifying(&[]));
        harness.faults.insert(1, Fault::BleStatus(0x05));
        let mut scripted = engine(vec![
            subscribe(PROGRESS, NotifyKind::Notification),
            random(1),
        ]);
        harness.run(&mut scripted).unwrap();
        harness.device.pending.push_back(progress(1));
        harness.run(&mut scripted).unwrap();
        assert!(scripted.notified.is_empty());
    }

    #[test]
    fn flags_a_subscription_the_driver_did_not_announce() {
        let mut harness = Harness::new(Pump::default());
        harness.announced = Some(&INFO);
        let mut scripted = engine(vec![subscribe(PROGRESS, NotifyKind::Indication)]);
        assert_eq!(
            harness.run(&mut scripted),
            Err(Violation::UnannouncedSubscribe(UUID, PROGRESS))
        );
        let mut scripted = engine(vec![subscribe(UUID, NotifyKind::Notification)]);
        assert_eq!(
            harness.run(&mut scripted),
            Err(Violation::UnannouncedSubscribe(UUID, UUID))
        );
        let mut scripted = engine(vec![subscribe(PROGRESS, NotifyKind::Notification)]);
        assert_eq!(harness.run(&mut scripted), Ok(()));
    }

    #[test]
    fn a_declared_resendable_write_may_be_sent_again() {
        let mut harness = Harness::new(Pump::default());
        harness.faults.insert(1, Fault::BleTimeout);
        harness.resendable.insert(UUID);
        let mut scripted = engine(vec![write(vec![9]), write(vec![9])]);
        assert_eq!(harness.run(&mut scripted), Ok(()));
    }

    fn write_to(characteristic: Uuid, data: Vec<u8>) -> Action {
        Action::Ble {
            id: ID,
            op: BleOp::Write {
                service: UUID,
                characteristic,
                data,
            },
        }
    }

    #[test]
    fn a_new_message_on_the_same_characteristic_clears_a_timed_out_frame() {
        let mut harness = Harness::new(Pump::default());
        harness.faults.insert(1, Fault::BleTimeout);
        let mut scripted = engine(vec![
            write(vec![1, 0]),
            write(vec![2, 0]),
            write(vec![1, 0]),
        ]);
        assert_eq!(harness.run(&mut scripted), Ok(()));
    }

    #[test]
    fn the_same_bytes_on_another_characteristic_do_not_clear_the_timeout() {
        let mut harness = Harness::new(Pump::default());
        harness.faults.insert(1, Fault::BleTimeout);
        let mut scripted = engine(vec![
            write(vec![9]),
            write_to(PROGRESS, vec![7]),
            write(vec![9]),
        ]);
        assert_eq!(
            harness.run(&mut scripted),
            Err(Violation::ResentAfterTimeout(vec![9]))
        );
    }

    #[cfg(feature = "cgm")]
    mod cgm {
        use super::*;
        use cgm_shared::{CgmCommand, CgmEngine, CgmEvent};

        #[derive(Default)]
        struct Cgm {
            actions: VecDeque<Action>,
            events: VecDeque<CgmEvent>,
            timeouts: VecDeque<Timestamp>,
            completions: usize,
            notified: usize,
        }

        impl CgmEngine for Cgm {
            fn submit(&mut self, _command: CgmCommand) -> Result<CommandId, SubmitError> {
                Ok(CommandId::new(1))
            }

            fn stop(&mut self) -> CommandId {
                CommandId::new(2)
            }

            fn handle_completion(
                &mut self,
                _id: OpId,
                _completion: Completion,
            ) -> Result<(), CompletionError> {
                self.completions += 1;
                Ok(())
            }

            fn handle_timeout(&mut self, _now: Timestamp) {
                self.timeouts.pop_front();
            }

            fn handle_wall_clock(&mut self, _now: UtcTime) {}

            fn handle_link_lost(&mut self) {}

            fn handle_notification(&mut self, _notification: Notification) {
                self.notified += 1;
            }

            fn poll_action(&mut self) -> Option<Action> {
                self.actions.pop_front()
            }

            fn poll_event(&mut self) -> Option<CgmEvent> {
                self.events.pop_front()
            }

            fn poll_timeout(&self) -> Option<Timestamp> {
                self.timeouts.front().copied()
            }

            fn state(&self) -> EngineState {
                EngineState::Paired
            }
        }

        #[test]
        fn drives_a_cgm_engine() {
            let mut cgm = Cgm {
                actions: vec![subscribe(PROGRESS, NotifyKind::Notification), read()].into(),
                events: vec![CgmEvent::BackfillAvailable {
                    after: pump_shared::Sequence::new(3),
                }]
                .into(),
                timeouts: vec![Timestamp::from_millis(10)].into(),
                ..Cgm::default()
            };
            let mut harness: Harness<_, CgmEvent> = Harness::new(pump_notifying(&[]));
            harness.run(&mut CgmHost(&mut cgm)).unwrap();
            harness.device.pending.push_back(progress(1));
            harness
                .advance(&mut CgmHost(&mut cgm), Duration::from_secs(1))
                .unwrap();
            assert_eq!(cgm.completions, 2);
            assert_eq!(cgm.notified, 1);
            assert!(cgm.timeouts.is_empty());
            assert_eq!(
                harness.events,
                [CgmEvent::BackfillAvailable {
                    after: pump_shared::Sequence::new(3)
                }]
            );
            assert_eq!(cgm.submit(CgmCommand::Status), Ok(CommandId::new(1)));
            assert_eq!(cgm.stop(), CommandId::new(2));
            cgm.handle_wall_clock(UtcTime::from_unix_millis(0));
            cgm.handle_link_lost();
            assert_eq!(cgm.state(), EngineState::Paired);
        }
    }
}
