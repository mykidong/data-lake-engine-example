package mykidong.http;

import mykidong.reflect.DynamicSparkRunner;
import org.apache.spark.api.java.JavaSparkContext;
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

    private JavaSparkContext jsc;
    private SparkSession spark;

    public SparkRequestHandlerServlet(JavaSparkContext jsc, SparkSession spark)
    {
        this.jsc = jsc;
        this.spark = spark;
    }

    protected void doPost(
            HttpServletRequest request,
            HttpServletResponse response)
            throws ServletException, IOException {

        // full class name.
        String fullClassName = request.getParameter("fullClassName");
        log.info("fullClassName: [" + fullClassName + "]");

        // codes parameter.
        String codes = request.getParameter("codes");
        log.info("codes: [" + codes + "]");

        // run spark codes dynamically.
        try {
            // set fair scheduler pool.
            jsc.setLocalProperty("spark.scheduler.pool", "production");

            long start = System.currentTimeMillis();

            DynamicSparkRunner sparkRunner = Reflect.compile(
                    fullClassName, codes).create().get();

            String result = sparkRunner.run(spark);

            log.info("elapsed time: [" + (double)(System.currentTimeMillis() - start) / (double)1000 + "]s");
            log.info("requested spark job is done...");

            // unset fair scheduler pool.
            jsc.setLocalProperty("spark.scheduler.pool", null);

            // response.
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println("{ \"result\": \"" + result + "\"}");
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
