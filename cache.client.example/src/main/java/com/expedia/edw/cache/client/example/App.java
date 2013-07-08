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
package com.expedia.edw.cache.client.example;

import com.expedia.edw.cache.client.CacheClientGson;

/**
 * @author Illya Yalovyy
 * @author Yaroslav Mykhaylov
 */
public class App {

    public static void main(String[] args) {
        CacheClientGson client = new CacheClientGson();
        /**
         * where {http://localhost:8080/rest/} - uri for rest cache
         * {sampleCache1} - region {key} - key for cache whole example:
         * http://localhost:8080/rest/sampleCache1/key/
         */
        client.setURL("http://localhost:8080/rest/sampleCache1/key/");
        System.out.println(client.fetchData("information_schema.CHARACTER_SETS", "CHARACTER_SET_NAME", "MAXLEN"));
    }
}
