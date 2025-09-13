use std::collections::HashSet;
use libp2p::PeerId;
use tokio::sync::RwLock;

#[derive(Default)]
pub struct Acl {
    allowed: RwLock<HashSet<PeerId>>,
}

impl Acl {
    pub fn new() -> Self { Self::default() }
    pub async fn allow(&self, pid: PeerId) { self.allowed.write().await.insert(pid); }
    pub async fn revoke(&self, pid: &PeerId) { self.allowed.write().await.remove(pid); }
    pub async fn is_allowed(&self, pid: &PeerId) -> bool { self.allowed.read().await.contains(pid) }
}
