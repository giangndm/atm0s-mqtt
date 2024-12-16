# Decentralized MQTT Broker

## Overview

A high-performance, decentralized MQTT broker built on top of
[atm0s_small_p2p](https://github.com/8xff/atm0s-small-p2p), designed for
scalable and resilient message routing across distributed systems.

## Key Features

- **Decentralized Architecture**: Each broker maintains its own topic registry
  and synchronizes subscriptions across the network
- **Peer-to-Peer Messaging**: Efficient message forwarding using atm0s-small-p2p
  unicast
- **EMQX Compatibility**: Provides HTTP APIs and Webhooks compatible with EMQX
- **Scalable Design**: Supports horizontal scaling and network resilience

## How It Works

### Topic and Subscription Management

- Each broker maintains an independent registry for MQTT topics and
  subscriptions
- Leverages atm0s-small-p2p for real-time synchronization across broker
  instances

### Message Routing

When a message is published:

1. The broker checks for local subscriptions
2. Identifies remote subscriptions through the p2p network
3. Forwards messages to subscribers using efficient unicast (relayed or not)
   mechanisms

## Installation

TODO

## Roadmap

- [ ] Support for MQTT 5.0
- [ ] Support for TLS
- [ ] Support for authentication
- [ ] Support for EMQX APIs and Webhooks

## Contributing

Contributions are welcome! Please read our contributing guidelines before
getting started.
