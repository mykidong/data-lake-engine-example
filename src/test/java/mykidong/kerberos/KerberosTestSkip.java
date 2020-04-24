package mykidong.kerberos;

import mykidong.SparkSQLTestSkip;
import mykidong.util.Log4jConfigurer;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosTestSkip {

    private static Logger log = LoggerFactory.getLogger(KerberosTestSkip.class);

    @Before
    public void init() throws Exception {
        // init. log4j.
        Log4jConfigurer.loadLog4j(null);
    }

    @Test
    public void login() throws Exception
    {
        try {
            String princiapl = "mykidong2/mc-d02.opasnet.io@OPASNET.IO";
            String keytab = "/etc/ozone/ozone.keytab";
            UserGroupInformation.loginUserFromKeytab(princiapl, keytab);

            log.info("login done with kerberos!");
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
