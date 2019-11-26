package mykidong;

import mykidong.util.Log4jConfigurer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringUtilsTestSkip {

    private static Logger log = LoggerFactory.getLogger(StringUtilsTestSkip.class);

    @Before
    public void init() throws Exception
    {
        Log4jConfigurer.loadLog4j(null);
    }

    @Test
    public void parseSemicolon() throws Exception
    {
        String location = "Location: ";

        int index = location.indexOf(":");

        if(index > 0)
        {
            location = location.substring(0, index);
        }

        log.info("location: " + location);

        Assert.assertTrue(location.equals("Location"));
    }
}
