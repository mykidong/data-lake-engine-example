package mykidong.http;

import mykidong.reflect.DynamicSparkRunner;
import org.apache.spark.sql.SparkSession;
import org.joor.Reflect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class SparkRequestHandlerServlet extends HttpServlet {

    private static Logger log = LoggerFactory.getLogger(SparkRequestHandlerServlet.class);

    private SparkSession spark;

    public SparkRequestHandlerServlet(SparkSession spark)
    {
        this.spark = spark;
    }

    protected void doPost(
            HttpServletRequest request,
            HttpServletResponse response)
            throws ServletException, IOException {

        // codes parameter.
        String codes = request.getParameter("codes");
        log.info("codes: [" + codes + "]");

        DynamicSparkRunner sparkRunner = Reflect.compile(
                "mykidong.SparkRunner", codes).create().get();

        // run spark codes dynamically.
        try {
            sparkRunner.run(spark);

            log.info("requested spark job is done...");

            // response.
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println("{ \"status\": \"ok\"}");
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
