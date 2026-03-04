import { Client as SSHClient, ConnectConfig } from 'ssh2';
import { Duplex } from 'stream';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { ConnectionProfile, ConnectionSecrets } from '../../core/types';

export interface SshTunnelResult {
    stream: Duplex;
    close: () => void;
}

/**
 * Expand ~ to home directory in file paths.
 */
function expandHome(filePath: string): string {
    if (filePath.startsWith('~/') || filePath === '~') {
        return path.join(os.homedir(), filePath.slice(1));
    }
    return filePath;
}

/**
 * Open an SSH tunnel to the target database host/port and return a forwarded stream.
 *
 * The returned stream can be passed directly to a database driver (pg, mysql2)
 * as a custom connection stream. Call `close()` after the database connection
 * is done to release SSH resources.
 */
export function openSshTunnel(
    profile: ConnectionProfile,
    secrets: ConnectionSecrets
): Promise<SshTunnelResult> {
    return new Promise((resolve, reject) => {
        if (!profile.sshHost) {
            return reject(createSshError('SSH_VALIDATION', 'SSH host is required.'));
        }
        if (!profile.sshUsername) {
            return reject(createSshError('SSH_VALIDATION', 'SSH username is required.'));
        }

        const dbHost = profile.host || 'localhost';
        const dbPort = profile.port || 5432;

        const sshConfig: ConnectConfig = {
            host: profile.sshHost,
            port: profile.sshPort || 22,
            username: profile.sshUsername,
            readyTimeout: 15000,
        };

        if (profile.sshAuthMethod === 'privateKey') {
            let keyContent: Buffer | undefined;

            if (secrets.sshPrivateKey) {
                keyContent = Buffer.from(secrets.sshPrivateKey);
            } else if (profile.sshPrivateKeyPath) {
                const keyPath = expandHome(profile.sshPrivateKeyPath);
                try {
                    keyContent = fs.readFileSync(keyPath);
                } catch (err: unknown) {
                    return reject(createSshError(
                        'SSH_INVALID_KEY',
                        `Cannot read SSH private key file: ${err instanceof Error ? err.message : String(err)}`
                    ));
                }
            }

            if (!keyContent) {
                return reject(createSshError(
                    'SSH_VALIDATION',
                    'SSH private key auth requires either a key file path or pasted key content.'
                ));
            }

            sshConfig.privateKey = keyContent;
            if (secrets.sshPrivateKeyPassphrase) {
                sshConfig.passphrase = secrets.sshPrivateKeyPassphrase;
            }
        } else {
            // password auth
            if (!secrets.sshPassword) {
                return reject(createSshError('SSH_VALIDATION', 'SSH password is required.'));
            }
            sshConfig.password = secrets.sshPassword;
        }

        const sshClient = new SSHClient();

        sshClient.on('ready', () => {
            sshClient.forwardOut(
                'localhost',
                0,
                dbHost,
                dbPort,
                (err, stream) => {
                    if (err) {
                        sshClient.end();
                        return reject(createSshError(
                            'SSH_TUNNEL_FAILED',
                            `SSH tunnel forwarding failed: ${err.message}`
                        ));
                    }

                    resolve({
                        stream,
                        close: () => {
                            try { stream.destroy(); } catch { /* ignore */ }
                            try { sshClient.end(); } catch { /* ignore */ }
                        }
                    });
                }
            );
        });

        sshClient.on('error', (err) => {
            const message = err.message || String(err);

            if (message.includes('Authentication') || message.includes('auth')) {
                return reject(createSshError('SSH_AUTH_FAILED', `SSH authentication failed: ${message}`));
            }
            if (message.includes('passphrase') || message.includes('decrypt')) {
                return reject(createSshError('SSH_INVALID_KEY', `Invalid SSH private key or passphrase: ${message}`));
            }
            reject(createSshError('SSH_HOST_UNREACHABLE', `SSH connection error: ${message}`));
        });

        sshClient.on('timeout', () => {
            sshClient.end();
            reject(createSshError('SSH_TIMEOUT', 'SSH connection timed out. Check SSH host and port.'));
        });

        sshClient.connect(sshConfig);
    });
}

/**
 * Create a coded error for SSH failures so they can be mapped in connectionErrors.
 */
function createSshError(code: string, message: string): Error & { code: string } {
    const err = new Error(message) as Error & { code: string };
    err.code = code;
    return err;
}
