package mykidong.kerberos;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

public class Kerberos {

    @Test
    public void login() throws Exception
    {
        try {
            String princiapl = "mykidong/mc-d02.opasnet.io@OPASNET.IO";
            String keytab = "/etc/ozone/ozone.keytab";
            UserGroupInformation.loginUserFromKeytab(princiapl, keytab);

            System.out.println("login done with kerberos!"):
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
