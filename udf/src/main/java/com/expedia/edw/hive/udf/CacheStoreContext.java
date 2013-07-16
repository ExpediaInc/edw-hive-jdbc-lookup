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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Illya Yalovyy
 * @author Yaroslav_Mykhaylov
 */
public class CacheStoreContext  {

//    private final Logger LOG = LoggerFactory.getLogger(CacheStoreAdapter.class);
    private boolean fileMode = false;
    private Strategy manager;
    private String URL;

    public CacheStoreContext(String URL) {
        this.URL = URL;
        manager = new MemoryStrategy(this.URL);
    }

    public void load(String schemaAndTableName, String keyName, String valueName) {
        try {
            manager.load(schemaAndTableName, keyName, valueName);

        } catch (OutOfMemoryError ex) {
            this.fileMode = true;
            manager = new FileStrategy(this.URL);

//            LOG.info("Set file strategy load cache");

            manager.load(schemaAndTableName, keyName, valueName);

        }
    }

    public void setFileMode(boolean fileMode) {
        this.fileMode = fileMode;
    }

    public boolean isFileMode() {
        return fileMode;
    }

    public String get(String key) {
        return manager.get(key);
    }
}
