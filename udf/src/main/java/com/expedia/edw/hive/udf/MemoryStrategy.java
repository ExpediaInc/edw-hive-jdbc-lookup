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
package com.expedia.edw.hive.udf;

import com.expedia.edw.cache.client.CacheClientGson;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Illya Yalovyy
 * @author Yaroslav_Mykhaylov
 */
public class MemoryStrategy implements Strategy {

    private CacheClientGson clientGson = new CacheClientGson();
    private Map<String, String> dataCache;

    public MemoryStrategy(String URL) {
        clientGson.setURL(URL);
        dataCache = new HashMap<String, String>();
    }

    public void load(String schemaAndTableName, String keyName, String valueName) {
        dataCache = clientGson.fetchData(schemaAndTableName, keyName, valueName);
    }

    public String get(String key) {
        return dataCache.get(key);
    }
}
