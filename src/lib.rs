mod spec {
    pub mod v1;
}
/// Designing everything for a pratical system is hard. Designing everything
/// correctly is impossible. That's why a versioned `spec` module is prepared.
///
/// At any time top level module does wildcard re-exporting from the latest
/// version of `spec`. To survive from a breaking change, change `use crate::*`
/// to `use crate::spec::v1::*`.
///
/// Components of the project target to all version of specification. If one
/// single item cannot co-implement every version, an alternative item will be
/// provided in the module of old verions of specification.
pub use spec::v1::*;

pub mod execute;
pub mod recv {
    pub mod unreplicated;
    pub use unreplicated::Unreplicated;
}
pub mod engine {
    #[cfg(test)]
    pub mod sim;
    pub mod udp;
}
pub mod app {
    #[cfg(test)]
    pub mod mock;
    #[cfg(test)]
    pub use mock::{Mock, Upcall};
    pub mod null;
    pub use null::Null;
}
pub mod util {
    pub mod timer;
    pub use timer::*;
    pub mod execute;
    pub use execute::*;
    pub mod misc;
}
