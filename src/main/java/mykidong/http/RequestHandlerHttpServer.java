package mykidong.http;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestHandlerHttpServer {

    private static Logger log = LoggerFactory.getLogger(RequestHandlerHttpServer.class);

    private Server server;
    private JavaSparkContext jsc;
    private SparkSession spark;
    private int port;

    public RequestHandlerHttpServer(int port, JavaSparkContext jsc, SparkSession spark)
    {
        this.port = port;
        this.jsc = jsc;
        this.spark = spark;
    }

    public void start() throws Exception {
        server = new Server();

        ServerConnector connector = new ServerConnector(server);
        connector.setPort(this.port);

        server.setConnectors(new Connector[] {connector});

        // add servlet to handler.
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(new ServletHolder(new SparkRequestHandlerServlet(jsc, spark)), "/run-codes");

        server.setHandler(handler);
        server.start();
    }
}
