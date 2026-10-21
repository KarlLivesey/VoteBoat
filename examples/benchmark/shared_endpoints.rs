// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Private benchmark endpoint ownership, including the unused protocol half.
use super::*;

pub(super) enum Endpoint {
    Tcp(TcpListener),
    #[cfg(feature = "quic")]
    Quic(UdpSocket),
}
pub(crate) struct Endpoints {
    #[cfg(test)]
    leases: Vec<crate::test_ports::PortLease>,
    #[cfg(test)]
    selected: [bool; 3],
    #[cfg(not(test))]
    pairs: Vec<(Option<TcpListener>, Option<UdpSocket>)>,
    addresses: BTreeMap<u64, std::net::SocketAddr>,
}
impl Endpoints {
    pub(crate) fn acquire() -> Result<Self, Failure> {
        #[cfg(test)]
        {
            let leases = (0..3)
                .map(|_| crate::test_ports::PortLease::acquire(1))
                .collect::<Vec<_>>();
            let addresses = (1..=3)
                .zip(leases.iter().map(|lease| lease.address_list()[0]))
                .collect();
            Ok(Self {
                leases,
                addresses,
                selected: [false; 3],
            })
        }
        #[cfg(not(test))]
        {
            let pairs = (0..3)
                .map(|_| {
                    (0..32)
                        .find_map(|_| {
                            let tcp = TcpListener::bind("127.0.0.1:0").ok()?;
                            let udp = UdpSocket::bind(tcp.local_addr().ok()?).ok()?;
                            Some((Some(tcp), Some(udp)))
                        })
                        .ok_or("no local TCP/UDP endpoint")
                })
                .collect::<Result<Vec<_>, _>>()?;
            let addresses = (1..=3)
                .zip(
                    pairs
                        .iter()
                        .map(|(t, _)| t.as_ref().unwrap().local_addr().unwrap()),
                )
                .collect();
            Ok(Self { pairs, addresses })
        }
    }
    pub(super) fn addresses(&self) -> &BTreeMap<u64, std::net::SocketAddr> {
        &self.addresses
    }
    pub(super) fn take(&mut self, n: u64, protocol: NativePeerProtocol) -> Endpoint {
        let slot = n as usize - 1;
        #[cfg(test)]
        {
            self.selected[slot] = true;
        }
        match protocol {
            NativePeerProtocol::TcpTls => {
                #[cfg(test)]
                let socket = self.leases[slot].adopt_tcp(0);
                #[cfg(not(test))]
                let socket = self.pairs[slot].0.take().expect("held TCP half");
                Endpoint::Tcp(socket)
            }
            #[cfg(feature = "quic")]
            NativePeerProtocol::Quic => {
                #[cfg(test)]
                let socket = self.leases[slot].adopt_udp(0);
                #[cfg(not(test))]
                let socket = self.pairs[slot].1.take().expect("held UDP half");
                Endpoint::Quic(socket)
            }
        }
    }
    /// All selected connectors must have closed before reclaim; unused halves
    /// and test registry locks have remained owned throughout.
    pub(crate) fn reclaim(&mut self) -> Result<(), Failure> {
        #[cfg(test)]
        for (lease, selected) in self.leases.iter_mut().zip(&mut self.selected) {
            if *selected {
                lease.reclaim();
                *selected = false;
            }
        }
        #[cfg(not(test))]
        for (n, (tcp, udp)) in (1..=3).zip(&mut self.pairs) {
            if tcp.is_none() {
                *tcp = Some(TcpListener::bind(self.addresses[&n])?);
            }
            if udp.is_none() {
                *udp = Some(UdpSocket::bind(self.addresses[&n])?);
            }
        }
        Ok(())
    }
}

pub(super) fn tcp(
    endpoint: Option<Endpoint>,
    address: std::net::SocketAddr,
) -> Result<TcpListener, Failure> {
    match endpoint {
        Some(Endpoint::Tcp(socket)) => Ok(socket),
        #[cfg(feature = "quic")]
        Some(Endpoint::Quic(_)) => Err("wrong prepared endpoint protocol".into()),
        None => Ok(TcpListener::bind(address)?),
    }
}
#[cfg(feature = "quic")]
pub(super) fn quic(
    endpoint: Option<Endpoint>,
    address: std::net::SocketAddr,
) -> Result<UdpSocket, Failure> {
    match endpoint {
        Some(Endpoint::Quic(socket)) => Ok(socket),
        Some(Endpoint::Tcp(_)) => Err("wrong prepared endpoint protocol".into()),
        None => Ok(UdpSocket::bind(address)?),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tcp_adoption_keeps_selected_and_unused_halves_owned_until_reclaim() {
        let _owner = crate::test_process::owner();
        let mut endpoints = Endpoints::acquire().unwrap();
        let address = endpoints.addresses()[&1];
        let prepared = endpoints.take(1, NativePeerProtocol::TcpTls);
        let witness = match &prepared {
            Endpoint::Tcp(socket) => socket.try_clone().unwrap(),
            #[cfg(feature = "quic")]
            Endpoint::Quic(_) => unreachable!(),
        };
        // A real competing kernel owner makes the historical rebind fail.
        assert_eq!(
            tcp(None, address)
                .unwrap_err()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .kind(),
            std::io::ErrorKind::AddrInUse
        );
        let adopted = tcp(Some(prepared), address).unwrap();
        assert!(UdpSocket::bind(address).is_err(), "unused UDP half escaped");
        assert_eq!(adopted.local_addr().unwrap(), address);
        drop(adopted);
        assert!(
            TcpListener::bind(address).is_err(),
            "witness did not hold the selected half"
        );
        drop(witness);
        endpoints.reclaim().unwrap();
        assert!(TcpListener::bind(address).is_err(), "reclaimed TCP escaped");
        drop(endpoints);
        drop(TcpListener::bind(address).unwrap());
        drop(UdpSocket::bind(address).unwrap());
    }
    #[cfg(feature = "quic")]
    #[test]
    fn rejected_adoption_releases_selected_half_before_partial_reclaim() {
        let _owner = crate::test_process::owner();
        let mut endpoints = Endpoints::acquire().unwrap();
        let address = endpoints.addresses()[&1];
        let prepared = endpoints.take(1, NativePeerProtocol::TcpTls);
        assert_eq!(
            quic(Some(prepared), address).unwrap_err().to_string(),
            "wrong prepared endpoint protocol"
        );
        assert!(
            UdpSocket::bind(address).is_err(),
            "unused half escaped after rejection"
        );
        endpoints.reclaim().unwrap();
        drop(endpoints);
        drop(TcpListener::bind(address).unwrap());
        drop(UdpSocket::bind(address).unwrap());
    }
    #[cfg(feature = "quic")]
    #[test]
    fn quic_adoption_keeps_selected_and_unused_halves_owned_until_reclaim() {
        let _owner = crate::test_process::owner();
        let mut endpoints = Endpoints::acquire().unwrap();
        let address = endpoints.addresses()[&1];
        let prepared = endpoints.take(1, NativePeerProtocol::Quic);
        let Endpoint::Quic(ref socket) = prepared else {
            unreachable!()
        };
        let witness = socket.try_clone().unwrap();
        assert_eq!(
            quic(None, address)
                .unwrap_err()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .kind(),
            std::io::ErrorKind::AddrInUse
        );
        let adopted = quic(Some(prepared), address).unwrap();
        assert!(
            TcpListener::bind(address).is_err(),
            "unused TCP half escaped"
        );
        assert_eq!(adopted.local_addr().unwrap(), address);
        drop(adopted);
        assert!(
            UdpSocket::bind(address).is_err(),
            "witness did not hold the selected half"
        );
        drop(witness);
        endpoints.reclaim().unwrap();
        assert!(UdpSocket::bind(address).is_err(), "reclaimed UDP escaped");
        drop(endpoints);
        drop(TcpListener::bind(address).unwrap());
        drop(UdpSocket::bind(address).unwrap());
    }
}
