package mykidong;

import mykidong.util.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class DynamicCodesRequestTestSkip {

    @Test
    public void requestWithCodes() throws Exception
    {
        // read java codes.
        String codes = StringUtils.fileToString("/templates/save-as-table-request.java");


        String url = "http://localhost:8125/run-codes";
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost httpPost = new HttpPost(url);

        List<NameValuePair> params = new ArrayList<NameValuePair>();
        params.add(new BasicNameValuePair("codes", codes));
        httpPost.setEntity(new UrlEncodedFormEntity(params));

        HttpResponse response = client.execute(httpPost);

        Assert.assertTrue(response.getStatusLine().getStatusCode() == 200);
    }
}
