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
package com.expedia.edw.cache.service;

import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;

/**
 * @author Illya Yalovyy
 * @author Yaroslav Mykhaylov
 */
public class CacheManagerMapJCS implements CacheManagerMap {

    private static final org.apache.log4j.Logger logger =
            org.apache.log4j.Logger.getLogger(CacheManagerMapJCS.class);
    private JCS cacheProviderJCS;

    public CacheManagerMapJCS(String cacheRegionName) {
        try {
//            JCS.setConfigFilename("WEB-INF/cache.ccf");
            cacheProviderJCS = JCS.getInstance(cacheRegionName);
        } catch (CacheException ex) {
           logger.error(ex);
        }
    }

    @Override
    public Object get(String key) {
        return cacheProviderJCS.get(key);
    }

    @Override
    public void put(String key, Object value) {
        try {
            cacheProviderJCS.put(key, value);
        } catch (CacheException ex) {
            logger.error("Error put new cache", ex);
        }
    }
}
