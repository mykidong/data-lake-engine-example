package mykidong;

import mykidong.util.Log4jConfigurer;
import mykidong.util.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DynamicCodesRequestTestSkip {

    private static Logger log = LoggerFactory.getLogger(DynamicCodesRequestTestSkip.class);

    private SparkSession spark;

    @Before
    public void init() throws Exception {
        // init. log4j.
        Log4jConfigurer.loadLog4j(null);
    }

    @Test
    public void requestWithCodes() throws Exception
    {
        String path = System.getProperty("path", "/templates/save-as-table-request.scala");
        String thread = System.getProperty("threads", "1");

        int threadCount = Integer.valueOf(thread);

        // read java codes.
        String codes = StringUtils.fileToString(path);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<String>> fList = new ArrayList<>();

        for(int i = 0; i < threadCount; i++)
        {
            Future<String> fTask = executor.submit(() -> {

                String url = "http://localhost:8125/run-codes";
                HttpClient client = HttpClientBuilder.create().build();
                HttpPost httpPost = new HttpPost(url);

                List<NameValuePair> params = new ArrayList<NameValuePair>();
                params.add(new BasicNameValuePair("fullClassName", "mykidong.spark.SparkRunner"));
                params.add(new BasicNameValuePair("codes", codes));
                httpPost.setEntity(new UrlEncodedFormEntity(params));

                HttpResponse response = client.execute(httpPost);

                int status = response.getStatusLine().getStatusCode();

                log.info("response: " + status);
                Assert.assertTrue(status == 200);

                return (status == 200) ? "Success" : "Failed";
            });

            fList.add(fTask);
        }

        for(Future<String> fTask : fList)
        {
            System.out.printf("response: %s\n", fTask.get());
        }

        executor.shutdown();
    }
}
