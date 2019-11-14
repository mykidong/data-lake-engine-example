package mykidong;


import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

public class DremioJdbcConnectionTestSkip {

    @Test
    public void connectToDremio() throws Exception
    {
        // URL parameters
        String url = "jdbc:dremio:direct=mc-d01.opasnet.io:31010";

        Properties properties = new Properties();
        properties.setProperty("user", "mykidong");
        properties.setProperty("password", "icarus0337");

        Connection connection = DriverManager.getConnection(url, properties);

        String sql = "SELECT * from \"mc-hive\".\"default\".student";
        PreparedStatement ps = connection.prepareStatement(sql);

        ResultSet rs = ps.executeQuery();

        while (rs.next()) {
            String name = rs.getString("name");
            int age = rs.getInt("age");

            System.out.printf("name: %s, age: %d\n", name, age);
        }
    }
}
