package org.apache.flink.runtime.security.token;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.hadoop.HadoopUserUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Provides Kerberos login functionality. */
public class KerberosLoginProvider {

    private static final Logger LOG = LoggerFactory.getLogger(KerberosLoginProvider.class);

    private final String principal;

    private final String keytab;

    private final boolean useTicketCache;

    public KerberosLoginProvider(Configuration configuration) {
        checkNotNull(configuration, "Flink configuration must not be null");
        SecurityConfiguration securityConfiguration = new SecurityConfiguration(configuration);
        this.principal = securityConfiguration.getPrincipal();
        this.keytab = securityConfiguration.getKeytab();
        this.useTicketCache = securityConfiguration.useTicketCache();
    }

    public boolean isLoginPossible() throws IOException {
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();

        if (principal != null) {
            LOG.debug("Login from keytab is possible");
            return true;
        } else if (!HadoopUserUtils.isProxyUser(currentUser)) {
            if (useTicketCache && currentUser.hasKerberosCredentials()) {
                LOG.debug("Login from ticket cache is possible");
                return true;
            }
        } else {
            throwProxyUserNotSupported();
        }

        LOG.debug("Login is NOT possible");

        return false;
    }

    public UserGroupInformation doLogin() throws IOException {
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();

        if (principal != null) {
            LOG.info(
                    "Attempting to login to KDC using principal: {} keytab: {}", principal, keytab);
            UserGroupInformation ugi =
                    UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
            LOG.info("Successfully logged into KDC");
            return ugi;
        } else if (!HadoopUserUtils.isProxyUser(currentUser)) {
            LOG.info("Attempting to load user's ticket cache");
            final String ccache = System.getenv("KRB5CCNAME");
            final String user =
                    Optional.ofNullable(System.getenv("KRB5PRINCIPAL"))
                            .orElse(currentUser.getUserName());
            UserGroupInformation ugi = UserGroupInformation.getUGIFromTicketCache(ccache, user);
            LOG.info("Loaded user's ticket cache successfully");
            return ugi;
        } else {
            throwProxyUserNotSupported();
            return currentUser;
        }
    }

    private void throwProxyUserNotSupported() {
        throw new UnsupportedOperationException("Proxy user is not supported");
    }
}