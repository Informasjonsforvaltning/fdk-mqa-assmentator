# FDK MQA Assmentator

This service is a component of the Metadata Quality Assessment (MQA) stack. It listens to dataset harvested events from 
Kafka and creates the initial quality assessment graph for datasets. The assessment graph is stored in a Data Quality 
Vocabulary (DQV) metrics model and published to the MQA event topic in Kafka for further processing.

For a broader understanding of the system’s context, refer to the [architecture documentation](https://github.com/Informasjonsforvaltning/architecture-documentation) wiki. For more specific 
context on this application, see the [Metadata Quality](https://github.com/Informasjonsforvaltning/architecture-documentation/wiki/Architecture-documentation#-metadata-quality) subsystem section.

## Getting Started
These instructions will give you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

Ensure you have the following installed:
- [Rust](https://www.rust-lang.org/tools/install)
- [Cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html)
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

Install required packages (Debian):

`sudo apt update && sudo apt install -y build-essential clang cmake libssl-dev pkg-config`

Install Rust:

`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`

### Format source code

`rustfmt --edition 2021 src/*`

### Running locally

Clone the repository:

```bash
git clone https://github.com/Informasjonsforvaltning/fdk-mqa-assmentator.git
cd fdk-mqa-assmentator
```

Build for development:

```
cargo build --verbose
```

Build release:

```
cargo build --release
```

Start Kafka (Docker Compose) and the application

```
docker compose up -d
./target/release/fdk-mqa-assmentator
```

Show help:

```
./target/release/fdk-mqa-assmentator --help
```

### Running tests

```
cargo test ./tests
```
