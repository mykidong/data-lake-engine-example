package mykidong;

import org.junit.Assert;
import org.junit.Test;

public class StringUtilsTestSkip {

    @Test
    public void parseSemicolon() throws Exception
    {
        String location = "Location: ";

        int index = location.indexOf(":");

        if(index > 0)
        {
            location = location.substring(0, index);
        }

        Assert.assertTrue(location.equals("Location"));
    }
}
