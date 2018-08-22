package com.exist.nifirestapi.util;

import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

@Component
public class Comm {

    public String httpUrlConnectionPut(String httpUrl, String jsonParam) {
        String result = "OK";
        URL url = null;
        try {
            url = new URL(httpUrl);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        if (url != null) {
            try {
                HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
                urlConn.setRequestProperty("content-type", "application/json");
                urlConn.setDoInput(true);
                urlConn.setDoOutput(true);
                urlConn.setConnectTimeout(5 * 1000);
                //设置请求方式为 PUT
                urlConn.setRequestMethod("PUT");
                urlConn.setRequestProperty("Content-Type", "application/json");
                urlConn.setRequestProperty("Accept", "application/json");
                urlConn.setRequestProperty("Charset", "UTF-8");
                DataOutputStream dos = new DataOutputStream(urlConn.getOutputStream());

                dos.writeBytes(jsonParam);
                dos.flush();
                dos.close();

                InputStreamReader isr = new InputStreamReader(urlConn.getInputStream());
                BufferedReader br = new BufferedReader(isr);
                String inputLine = null;
                while ((inputLine = br.readLine()) != null) {
                    result += inputLine;
                }
                isr.close();
                urlConn.disconnect();


            } catch (Exception e) {
                return e.getMessage();
            }
        }
        return result;

    }
}
