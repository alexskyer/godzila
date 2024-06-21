package org.apache.flink.runtime.security.token;
import org.apache.hadoop.security.Credentials;

/**
 * Manager for delegation tokens in a Flink cluster.
 *
 * <p>When delegation token renewal is enabled, this manager will make sure long-running apps can
 * run without interruption while accessing secured services. It must contact all the configured
 * secure services to obtain delegation tokens to be distributed to the rest of the application.
 */
public interface DelegationTokenManager {

    /**
     * Obtains new tokens in a one-time fashion and leaves it up to the caller to distribute them.
     */
    void obtainDelegationTokens(Credentials credentials) throws Exception;

    /**
     * Creates a re-occurring task which obtains new tokens and automatically distributes them to
     * task managers.
     */
    void start() throws Exception;

    /** Stops re-occurring token obtain task. */
    void stop();
}
