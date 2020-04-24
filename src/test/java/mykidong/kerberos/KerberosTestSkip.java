package mykidong.kerberos;

import mykidong.SparkSQLTestSkip;
import mykidong.util.Log4jConfigurer;
import org.apache.hadoop.conf.Configuration;
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
            String princiapl = "mykidong/mc-d02.opasnet.io@OPASNET.IO";
            String keytab = "/etc/ozone/ozone.keytab";

            Configuration conf = new Configuration();
            conf.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(conf);

            UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(princiapl, keytab);
            if(ugi == null) {
                ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(princiapl, keytab);
                log.info("login done with kerberos!");
            } else {
                log.info("ugi: " + ugi.toString());

                ugi.checkTGTAndReloginFromKeytab();
                log.info("check tgt done with kerberos!");
            }
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
