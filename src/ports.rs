//! UDP port allocation. Each leg takes an even RTP port and the odd port after it for RTCP.

use std::collections::VecDeque;
use std::io;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::sync::Mutex;

pub struct PortPool {
    ip: IpAddr,
    free: Mutex<VecDeque<u16>>,
    total: usize,
}

/// The RTP and RTCP sockets of one leg, bound but not yet registered with tokio.
pub struct LegSockets {
    pub port: u16,
    pub rtp: UdpSocket,
    pub rtcp: UdpSocket,
}

impl PortPool {
    /// Uses every even port `p` in `min..=max` whose `p + 1` is also in range.
    pub fn new(ip: IpAddr, min: u16, max: u16) -> io::Result<PortPool> {
        let first = min + (min % 2);
        let free: VecDeque<u16> = (first..max).step_by(2).collect();
        if free.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("port range {min}-{max} holds no RTP/RTCP pair"),
            ));
        }
        let total = free.len();
        Ok(PortPool { ip, free: Mutex::new(free), total })
    }

    /// Binds the next free pair. Ports held by another process are skipped and go back to the
    /// end of the queue.
    pub fn allocate(&self) -> io::Result<LegSockets> {
        let attempts = self.total;
        for _ in 0..attempts {
            let Some(port) = self.free.lock().unwrap().pop_front() else { break };
            match self.bind_pair(port) {
                Ok(sockets) => return Ok(sockets),
                Err(e) => {
                    tracing::warn!(port, error = %e, "port pair unavailable, skipping");
                    self.release(port);
                }
            }
        }
        Err(io::Error::new(io::ErrorKind::AddrNotAvailable, "no free media ports"))
    }

    /// Returns a port to the end of the queue, so a just-closed port isn't reused at once.
    pub fn release(&self, port: u16) {
        self.free.lock().unwrap().push_back(port);
    }

    pub fn available(&self) -> usize {
        self.free.lock().unwrap().len()
    }

    pub fn total(&self) -> usize {
        self.total
    }

    fn bind_pair(&self, port: u16) -> io::Result<LegSockets> {
        let rtp = UdpSocket::bind(SocketAddr::new(self.ip, port))?;
        let rtcp = UdpSocket::bind(SocketAddr::new(self.ip, port + 1))?;
        rtp.set_nonblocking(true)?;
        rtcp.set_nonblocking(true)?;
        Ok(LegSockets { port, rtp, rtcp })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_even_ports_with_room_for_rtcp() {
        let pool = PortPool::new("127.0.0.1".parse().unwrap(), 41001, 41006).unwrap();
        // 41002/41003 and 41004/41005; 41006 has no 41007 inside the range.
        assert_eq!(pool.total(), 2);
        let a = pool.allocate().unwrap();
        assert_eq!(a.port, 41002);
        assert_eq!(a.rtcp.local_addr().unwrap().port(), 41003);
        let b = pool.allocate().unwrap();
        assert_eq!(b.port, 41004);
        assert!(pool.allocate().is_err());
        drop(a);
        pool.release(41002);
        assert_eq!(pool.allocate().unwrap().port, 41002);
    }

    #[test]
    fn rejects_empty_range() {
        assert!(PortPool::new("127.0.0.1".parse().unwrap(), 41010, 41010).is_err());
    }
}
