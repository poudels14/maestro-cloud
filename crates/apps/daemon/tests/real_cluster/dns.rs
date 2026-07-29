use std::io;
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const DNS_PORT: u16 = 53;
const FIXTURE_ADDRESS: Ipv4Addr = Ipv4Addr::new(203, 0, 113, 53);

pub(super) struct LocalDnsUpstream {
    recursion_observed: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    worker: Option<JoinHandle<Result<(), io::Error>>>,
}

impl LocalDnsUpstream {
    pub(super) fn bind(address: Ipv4Addr) -> Result<Self, io::Error> {
        let address = SocketAddrV4::new(address, DNS_PORT);
        let socket = UdpSocket::bind(address)?;
        socket.set_read_timeout(Some(Duration::from_millis(100)))?;
        let recursion_observed = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let worker_recursion_observed = recursion_observed.clone();
        let worker_shutdown = shutdown.clone();
        let worker = thread::Builder::new()
            .name("maestro-real-cluster-dns".to_owned())
            .spawn(move || serve(socket, &worker_recursion_observed, &worker_shutdown))?;
        Ok(Self {
            recursion_observed,
            shutdown,
            worker: Some(worker),
        })
    }

    pub(super) fn fixture_address(&self) -> Ipv4Addr {
        FIXTURE_ADDRESS
    }

    pub(super) fn recursion_observed(&self) -> bool {
        self.recursion_observed.load(Ordering::Acquire)
    }

    pub(super) fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for LocalDnsUpstream {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn serve(
    socket: UdpSocket,
    recursion_observed: &AtomicBool,
    shutdown: &AtomicBool,
) -> Result<(), io::Error> {
    let mut request = [0_u8; 4_096];
    while !shutdown.load(Ordering::Acquire) {
        match socket.recv_from(&mut request) {
            Ok((length, source)) => {
                let Some(request) = request.get(..length) else {
                    continue;
                };
                if let Some((response, recursive)) = response(request) {
                    recursion_observed.fetch_or(recursive, Ordering::AcqRel);
                    socket.send_to(&response, source)?;
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn response(request: &[u8]) -> Option<(Vec<u8>, bool)> {
    if request.len() < 12 || request.get(4..6) != Some(&[0, 1]) {
        return None;
    }
    let question_end = question_end(request)?;
    let recursive = request.get(2).is_some_and(|flags| flags & 1 == 1);
    let mut response = Vec::with_capacity(question_end.saturating_add(16));
    response.extend_from_slice(request.get(0..2)?);
    response.extend_from_slice(if recursive {
        &[0x81, 0x80]
    } else {
        &[0x80, 0x02]
    });
    response.extend_from_slice(&[0, 1]);
    response.extend_from_slice(if recursive { &[0, 1] } else { &[0, 0] });
    response.extend_from_slice(&[0, 0, 0, 0]);
    response.extend_from_slice(request.get(12..question_end)?);
    if recursive {
        response.extend_from_slice(&[
            0xc0, 0x0c, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x3c, 0x00, 0x04,
        ]);
        response.extend_from_slice(&FIXTURE_ADDRESS.octets());
    }
    Some((response, recursive))
}

fn question_end(request: &[u8]) -> Option<usize> {
    let mut cursor = 12_usize;
    loop {
        let label_length = usize::from(*request.get(cursor)?);
        cursor = cursor.checked_add(1)?;
        if label_length == 0 {
            return cursor.checked_add(4).filter(|end| *end <= request.len());
        }
        if label_length & 0xc0 != 0 {
            return None;
        }
        cursor = cursor.checked_add(label_length)?;
        if cursor >= request.len() {
            return None;
        }
    }
}
