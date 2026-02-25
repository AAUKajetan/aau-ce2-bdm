# Hadoop Cluster Setup

## Overview
This document describes the Hadoop cluster configuration with 1 master node and 4 worker nodes.

## Cluster Nodes

| IP Address    | Hostname         | Role   |
|---------------|------------------|--------|
| 10.92.0.131   | hadoop-master    | Master |
| 10.92.0.211   | hadoop-worker1   | Worker |
| 10.92.0.87    | hadoop-worker2   | Worker |
| 10.92.0.160   | hadoop-worker3   | Worker |
| 10.92.0.16    | hadoop-worker4   | Worker |

## Authentication

### SSH Keys
All nodes use the SSH key named **mac2-bdm**:
- Private key: `id_ed25519_nsc`
- Public key: `id_ed25519_nsc.pub`

### User Credentials
- **Username**: `hadoop`
- **Password**: `hadoop`
