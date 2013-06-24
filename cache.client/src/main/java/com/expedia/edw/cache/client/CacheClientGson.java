/*
 * Copyright 2013 Expedia, Inc. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.epam.cache.client;

import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Illya Yalovyy
 * @author Yaroslav Mykhaylov
 */
public class CacheClientGson {

    private String URL;
    private static final String SEPARATOR = "/";

    public Map<String, String> fetchData(String schemaAndTableName, String keyName, String valueName) {
        String reqURL = URL + schemaAndTableName + SEPARATOR + keyName + SEPARATOR + valueName;
        Gson gson = new Gson();
        System.out.println(reqURL);
        String json = getHTML(reqURL, "GET");
        System.out.println(json);
        Map<String, String> map = new HashMap<String, String>();
        map = (Map<String, String>) gson.fromJson(json, map.getClass());
        return map;
    }

    private String getHTML(String urlToRead, String requestType) {
        URL url = null;
        HttpURLConnection conn = null;
        BufferedReader rd = null;
        String line = "";
        String result = "";
        try {
            url = new URL(urlToRead);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(requestType);
            rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            while ((line = rd.readLine()) != null) {
                result += line;
            }
            rd.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public String getURL() {
        return URL;
    }

    public void setURL(String URL) {
        this.URL = URL;
    }
}