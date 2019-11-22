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
import org.apache.http.util.EntityUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DynamicCodesRequestTestSkip {

    private static Logger log = LoggerFactory.getLogger(DynamicCodesRequestTestSkip.class);

    private SparkSession spark;

    @Before
    public void init() throws Exception {
        // init. log4j.
        Log4jConfigurer log4j = new Log4jConfigurer();
        log4j.setConfPath("/log4j.xml");
        log4j.afterPropertiesSet();
    }

    @Test
    public void requestWithCodes() throws Exception
    {
        // read java codes.
        String codes = StringUtils.fileToString("/templates/save-as-table-request.java");


        String url = "http://localhost:8125/run-codes";
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost httpPost = new HttpPost(url);

        List<NameValuePair> params = new ArrayList<NameValuePair>();
        params.add(new BasicNameValuePair("fullClassName", "mykidong.spark.SparkRunner"));
        params.add(new BasicNameValuePair("codes", codes));
        httpPost.setEntity(new UrlEncodedFormEntity(params));

        HttpResponse response = client.execute(httpPost);
        log.info("response: " + response.getStatusLine().getStatusCode());

        Assert.assertTrue(response.getStatusLine().getStatusCode() == 200);
    }


    @Test
    public void requestCountWithCodes() throws Exception
    {
        // read java codes.
        String codes = StringUtils.fileToString("/templates/count.java");

        String url = "http://localhost:8125/run-codes";
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost httpPost = new HttpPost(url);

        List<NameValuePair> params = new ArrayList<NameValuePair>();
        params.add(new BasicNameValuePair("fullClassName", "mykidong.spark.CountRunner"));
        params.add(new BasicNameValuePair("codes", codes));
        httpPost.setEntity(new UrlEncodedFormEntity(params));

        HttpResponse response = client.execute(httpPost);
        String responseString = EntityUtils.toString(response.getEntity(), "UTF-8");
        log.info("response: " + responseString);

        Assert.assertTrue(response.getStatusLine().getStatusCode() == 200);
    }
}
