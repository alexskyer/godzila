package org.apache.flink.runtime.security.token;


import org.apache.flink.annotation.Experimental;
import org.apache.flink.configuration.Configuration;

import org.apache.hadoop.security.Credentials;

import java.util.Optional;

/**
 * Delegation token provider API. Instances of token providers are loaded by {@link
 * DelegationTokenManager} through service loader.
 */
@Experimental
public interface DelegationTokenProvider {
    /** Name of the service to provide delegation tokens. This name should be unique. */
    String serviceName();

    /**
     * Called by {@link DelegationTokenManager} to initialize provider after construction.
     *
     * @param configuration Configuration to initialize the provider.
     */
    void init(Configuration configuration) throws Exception;

    /**
     * Return whether delegation tokens are required for this service.
     *
     * @return true if delegation tokens are required.
     */
    boolean delegationTokensRequired() throws Exception;

    /**
     * Obtain delegation tokens for this service.
     *
     * @param credentials Credentials to add tokens and security keys to.
     * @return If the returned tokens are renewable and can be renewed, return the time of the next
     *     renewal, otherwise `Optional.empty()` should be returned.
     */
    Optional<Long> obtainDelegationTokens(Credentials credentials) throws Exception;
}
