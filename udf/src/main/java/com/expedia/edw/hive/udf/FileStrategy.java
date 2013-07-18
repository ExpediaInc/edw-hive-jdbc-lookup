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

import com.expedia.edw.cache.client.CacheClientGsonWriteToFile;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.LoggerFactory;

/**
 * @author Illya Yalovyy
 * @author Yaroslav_Mykhaylov
 */
public class FileStrategy implements Strategy {

    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(CacheStoreContext.class);
    private CacheClientGsonWriteToFile clientCache;
    private JobConf job;
    private String cacheFileName;
    private String PATTERN_SEARCH = "\":\"([^,^}]*)\"";

    public FileStrategy(String URL) {
        clientCache = new CacheClientGsonWriteToFile();
        clientCache.setURL(URL);
    }

    public void load(String schemaAndTableName, String keyName, String valueName) {
        job = new JobConf();
        
        File cacheData = clientCache.fetchData(schemaAndTableName, keyName, valueName);
        cacheFileName = cacheData.getName();
        
        DistributedCache.addCacheFile(cacheData.toURI(), this.job);

    }

    public String get(String key) {
        try {
            URI[] uris = DistributedCache.getCacheFiles(job);

            for (int i = 0; i < uris.length; i++) {
                if (uris[i].toString().contains(cacheFileName)) {
                    return searchContainsKeyInFile(uris[i], key);
                }
            }

        } catch (IOException ex) {
            LOG.error("Cannot get key: " + key, ex);
        }
        return "";
    }

    private String searchContainsKeyInFile(URI uri, String key) {
        try {
            
            Scanner scanner = new Scanner(new File(uri));
            return getValue(scanner.findWithinHorizon(key + PATTERN_SEARCH, 0));

        } catch (IOException ex) {
            LOG.error("Problem with open distribution cache file: " + uri, ex);
        }
        return "";
    }

    private String getValue(String str) {
        Pattern p = Pattern.compile(PATTERN_SEARCH);
        Matcher m = p.matcher(str);
        
        if (m.find()) {
            // 1 - first contains
            return m.group(1);
        }
        return "";
    }
}
