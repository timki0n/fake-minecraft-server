import 'dotenv/config';
import {createServer} from 'node:net';
import {readFileSync} from 'node:fs';
import {log, formatAddress} from './utils.js';
import {ByteBuf, readHandshake, readLoginStart, writeStringPacket, PACKET_PONG} from './mc-protocol.js';

const HANDSHAKE_TIMEOUT = 2000; // ms
const N8N_WEBHOOK_URL = process.env.N8N_WEBHOOK_URL || null;

function main() {
    const server = createServer();

    // Add server lifecycle logging
    server.on('listening', () => {
        const {address, port} = server.address();
        log(`Listening on ${formatAddress(address)}:${port}`);
        if (N8N_WEBHOOK_URL) {
            log(`N8N Webhook: ${N8N_WEBHOOK_URL}`);
        }
    });
    server.on('error', (err) => {
        log('Server Error:', err);
    });

    // Add socket connection handler
    server.on('connection', handleSocket);

    // Start listening
    const port =
    Number.parseInt(process.env.PORT, 10) ||
    Number.parseInt(process.env.SERVER_PORT, 10) ||
    Number.parseInt(process.env.LISTEN_PORT, 10) ||
    25565;

    const host = process.env.LISTEN_HOST || '0.0.0.0';

    const listenOpts = {
    host,
    port,
    backlog: Number.parseInt(process.env.LISTEN_BACKLOG, 10) || undefined,
    };
    const listenErrorHandler = () => process.exit(1);
    server.on('error', listenErrorHandler);
    server.listen(listenOpts, () => server.off('error', listenErrorHandler));

    // Setup graceful shutdown
    setupGracefulShutdown(server);
}

/**
 * Sets up graceful shutdown handlers for SIGTERM and SIGINT signals.
 *
 * @param {net.Server} server - The server instance to shut down.
 */
function setupGracefulShutdown(server) {
    let isShuttingDown = false;

    const shutdown = (signal) => {
        if (isShuttingDown) {
            return;
        }
        isShuttingDown = true;

        log(`Received ${signal}, starting graceful shutdown...`);

        // Stop accepting new connections
        server.close((err) => {
            if (err) {
                log(`Error during server close: ${err.message}`);
                process.exit(1);
            }
            log('Server closed successfully');
            process.exit(0);
        });

        // Force shutdown after timeout (10 seconds)
        const SHUTDOWN_TIMEOUT = 10000;
        setTimeout(() => {
            log('Shutdown timeout reached, forcing exit');
            process.exit(1);
        }, SHUTDOWN_TIMEOUT).unref();
    };

    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));
}

/**
 * Handles a socket connection.
 *
 * @param {net.Socket} socket - The client socket.
 */
function handleSocket(socket) {
    const name = `[${formatAddress(socket.remoteAddress, false)}]:${socket.remotePort}`;

    // Add socket lifecycle logging
    log(`${name} connected`);
    let answered = false;
    let sockerErr = undefined;
    socket.on('error', (err) => {
        // log('Socket Error:', err); // debug
        if (!sockerErr) {
            sockerErr = err;
        }
    });
    socket.on('close', () => {
        if (sockerErr) {
            log(`${name} disconnected with error: ${sockerErr.message}`);
        } else if (!answered) {
            log(`${name} disconnected before receiving response`);
        } else {
            log(`${name} disconnected successfully`);
        }
    });

    // Ensure that the socket is destroyed immediately on error
    socket.on('error', () => socket.destroy());

    // Set a strict timeout for the handshake
    const timeoutTask = setTimeout(() => socket.destroy(new Error('Timeout')), HANDSHAKE_TIMEOUT);
    socket.on('close', () => clearTimeout(timeoutTask));

    // Add data handler
    const buf = new ByteBuf();
    let handshakeData = null; // Store handshake for login state
    socket.on('data', (data) => {
        if (socket.readyState !== 'open') {
            // always skip data after a call to socket.end()
            // without this, already received data sometimes continues to be read for a short time
            return;
        }
        if (answered) {
            return; // skip (already answered)
        }

        buf.append(data);

        // If we haven't received handshake yet, try to read it
        if (!handshakeData) {
            buf.resetOffset();
            const handshake = readHandshake(buf);
            if (handshake === undefined) {
                return; // skip (missing data)
            }
            if (handshake === false) {
                // fail (illegal handshake)
                socket.destroy(new Error('Illegal handshake'));
                return;
            }

            log(`${name} sent handshake: ${JSON.stringify(handshake)}`);

            // For status requests, respond immediately
            if (handshake.state === 1) {
                answered = true;
                socket.write(getServerListPacket(handshake));
                socket.end(PACKET_PONG);
                return;
            }

            // For login requests, store handshake and wait for Login Start packet
            handshakeData = handshake;
            // Remove processed handshake data from buffer
            buf.data = buf.data.slice(buf.offset);
            buf.resetOffset();
        }

        // We're in login state, try to read Login Start packet
        buf.resetOffset();
        const loginStart = readLoginStart(buf);
        if (loginStart === undefined) {
            return; // skip (missing data)
        }
        if (loginStart === false) {
            // fail (illegal login start)
            socket.destroy(new Error('Illegal login start'));
            return;
        }

        // Login attempt - log detailed client information
        answered = true;
        const loginData = {
            username: loginStart.username,
            uuid: loginStart.uuid || null,
            clientIp: socket.remoteAddress,
            clientPort: socket.remotePort,
            targetHost: handshakeData.hostname,
            targetPort: handshakeData.port,
            protocolVersion: handshakeData.protocolVersion,
            timestamp: new Date().toISOString(),
        };

        log(`========== LOGIN ATTEMPT ==========`);
        log(`Username: ${loginData.username}`);
        if (loginData.uuid) {
            log(`UUID: ${loginData.uuid}`);
        }
        log(`Client IP: ${loginData.clientIp}`);
        log(`Client Port: ${loginData.clientPort}`);
        log(`Target Host: ${loginData.targetHost}`);
        log(`Target Port: ${loginData.targetPort}`);
        log(`Protocol Version: ${loginData.protocolVersion}`);
        log(`===================================`);

        // Fetch kick message from n8n webhook (async)
        fetchKickMessage(loginData.username).then((kickMessage) => {
            socket.end(getKickPacket(handshakeData, kickMessage));
        });
    });
}

/**
 * Fetches kick message from n8n webhook.
 * Returns dynamic message based on server status (sleeping, maintenance, no auth, etc.)
 *
 * @param {string} username - The player's username.
 * @returns {Promise<string|null>} - The kick message from webhook or null if unavailable.
 */
async function fetchKickMessage(username) {
    if (!N8N_WEBHOOK_URL) {
        return null;
    }

    try {
        const url = new URL(N8N_WEBHOOK_URL);
        url.searchParams.set('username', username);
        
        const response = await fetch(url.toString(), {
            method: 'GET',
            headers: {
                'Accept': 'application/json',
            },
        });

        if (response.ok) {
            const data = await response.json();
            log(`Webhook response for ${username}: ${JSON.stringify(data)}`);
            
            // Support multiple response formats:
            // { "message": "..." } or { "kick_message": "..." } or { "text": "..." }
            const kickMessage = data.message || data.kick_message || data.text || null;
            return kickMessage;
        } else {
            log(`Webhook failed: ${response.status} ${response.statusText}`);
            return null;
        }
    } catch (err) {
        log(`Webhook error: ${err.message}`);
        return null;
    }
}

function parseChatComponent(str) {
    if (str.charAt(0) === '{') {
        try {
            return JSON.parse(str);
        } catch (ignored) {}
    }
    return {text: str};
}

function readFavicon(strOrPath) {
    if (!strOrPath) {
        return undefined;
    }

    if (strOrPath.startsWith('data:')) {
        return strOrPath;
    }

    try {
        const data = readFileSync(strOrPath, {encoding: 'base64'});
        return `data:image/png;base64,${data}`;
    } catch (err) {
        log(`Cannot read favicon: ${err.message}`);
        return undefined;
    }
}

const DEFAULT_KICK_MESSAGE = process.env.KICK_MESSAGE || '§cNot available';

/**
 * Creates a kick packet with the given message.
 * If dynamicMessage is provided, uses it; otherwise falls back to KICK_MESSAGE env.
 *
 * @param {Object} handshake - The handshake data.
 * @param {string|null} dynamicMessage - Dynamic message from webhook (optional).
 * @returns {Buffer} - The kick packet.
 */
function getKickPacket(handshake, dynamicMessage = null) {
    const message = dynamicMessage || DEFAULT_KICK_MESSAGE;
    return writeStringPacket(0, JSON.stringify(parseChatComponent(message)));
}

const getServerListPacket = (() => {
    // TODO: Allow PROTOCOL_VERSION=auto to copy the client's one
    const packet = writeStringPacket(
        0,
        JSON.stringify({
            version: {
                name: process.env.PROTOCOL_NAME || '',
                protocol: parseInt(process.env.PROTOCOL_VERSION) || 0,
            },
            players: {
                max: parseInt(process.env.MAX_PLAYERS) || 0,
                online: parseInt(process.env.ONLINE_PLAYERS) || 0,
                sample: [],
            },
            description: parseChatComponent(process.env.MOTD || '§eHello World!'),
            favicon: readFavicon(process.env.FAVICON),
        })
    );
    return (handshake) => packet;
})();

main();
