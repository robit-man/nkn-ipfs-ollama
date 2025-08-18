# nkn-ipfs-ollama

**A simple demonstration of streaming responses between a frontend hosted via IPFS and a backend running on Ollama, facilitated by Network Knowledge Network (NKN).**

[![Build Status](https://github.com/robit-man/nkn-ipfs-ollama/actions/workflows/main.yml/badge.svg)](https://github.com/robit-man/nkn-ipfs-ollama/actions)
[![License: MIT](https://img.shields.io/github/license/robit-man/nkn-ipfs-ollama)](https://github.com/robit-man/nkn-ipfs-ollama/blob/main/LICENSE)

## Overview

This project demonstrates a basic communication flow between a frontend application and a backend service, leveraging the following technologies:

*   **IPFS:** For decentralized storage and hosting of the frontend application.
*   **Ollama:** A local LLM serving platform.
*   **Network Knowledge Network (NKN):**  Used for efficient, low-latency communication between the frontend and backend, bypassing traditional network infrastructure.
*   **Python:**  The core language used for both the signaling server and the frontend.

## Architecture

The application consists of two main components:

1.  **Frontend (IPFS Hosted):**  A simple HTML page served via IPFS.  This page interacts with the NKN signaling server to initiate and manage the streaming response.
2.  **Signaling Server (NKN):**  A Python script (`ollama_nkn_server.py`) that bootstraps a virtual environment, initializes the NKN network, and manages the state of the NKN connection to the Ollama backend.  It handles the signaling and ensures the connection is established.

## Getting Started

**Prerequisites:**

*   **Ollama:**  Install Ollama following the instructions on [https://ollama.com/](https://ollama.com/).
*   **NKN:**  Ensure NKN is properly installed and running.  Refer to the NKN documentation for installation instructions: [https://networkknowledge.com/nkn/](https://networkknowledge.com/nkn/)
*   **Node.js & npm (or yarn/pnpm):** Required for frontend development.

**Steps:**

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/robit-man/nkn-ipfs-ollama
    cd nkn-ipfs-ollama
    ```

2.  **Set up the Frontend:**
    *   Navigate to the `site` directory: `cd site`
    *   Install dependencies: `npm install` (or `yarn install` or `pnpm install`)
    *   Run the frontend server: `python serve.py`  This will start a local HTTPS server with SSL.  You can then access the frontend in your browser at the address provided in the console output (usually `https://localhost:8000`).

3.  **Run the Signaling Server:**
    *   Navigate to the root directory: `cd ..`
    *   Run the signaling server: `python serve.py` This will start the NKN signaling server.

4.  **Interact:**
    *   Open the frontend in your browser (as described above).
    *   The frontend will initiate a streaming response from the Ollama backend via NKN.

## Key Files

*   `site/index.html`: The HTML file for the frontend application.
*   `site/serve.py`:  Python script to bootstrap the frontend environment and launch the HTTPS server.
*   `ollama_nkn_server.py`: Python script for the NKN signaling server.

## Deployment

*   **IPFS Hosting:**  The `index.html` file can be uploaded to IPFS.  You can use the IPFS Desktop application or the command-line `ipfs` tool.
*   **Ollama Backend:** The `ollama_nkn_server.py` script will automatically manage the Ollama backend.

## Contributing

We welcome contributions to this project!  Please see the [CONTRIBUTING.md](CONTRIBUTING.md) file for guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
