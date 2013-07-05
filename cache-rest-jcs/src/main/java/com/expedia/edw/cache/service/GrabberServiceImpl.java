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

import com.expedia.edw.cache.dao.GrabberDao;
import com.google.gson.Gson;
import java.util.Map;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author Illya Yalovyy
 * @author Yaroslav Mykhaylov
 */
@Service
public class GrabberServiceImpl implements GrabberService {

    private static final Logger logger =
            Logger.getLogger(GrabberServiceImpl.class);
    @Autowired
    private GrabberDao grabberDao;

    private CacheManagerMap manager = new CacheManagerMapJCS("defaultCache");
    private Gson clientGson = new Gson();

    @Override
    public String getData(String schemaAndDBname, String keyName, String valueName) {

        String keyCache = schemaAndDBname + keyName + valueName;
        String valueCache = "";


        logger.info("Get data for key: " + keyCache);

        valueCache = (String) manager.get(keyCache);

        if (valueCache == null) {
            logger.info("Not found cache for key: " + keyCache);

            Map<String, String> data = grabberDao.
                    getData(schemaAndDBname, keyName, valueName);

            if (data == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Not found data in database for key: " + keyCache);
                }
                return null;
            }

            valueCache = clientGson.toJson(data);
            manager.put(keyCache, valueCache);

            logger.info("Added data to cache for key: " + keyCache);
        }

        logger.info("Get cache for key: " + keyCache);
        return valueCache;
    }
}
