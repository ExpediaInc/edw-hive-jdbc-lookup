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
import com.expedia.edw.cache.dao.GrabberDaoJDBC;
import com.google.gson.Gson;
import java.util.Map;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import org.slf4j.LoggerFactory;

/**
 * @author Illya Yalovyy
 * @author Yaroslav Mykhaylov
 */
public class GrabberServiceImpl implements GrabberService {

    private final org.slf4j.Logger LOG =
            LoggerFactory.getLogger(GrabberServiceImpl.class);
    private GrabberDao grabberDao = new GrabberDaoJDBC();
    private Gson clientGson = new Gson();

    @Override
    public Element getData(String schemaAndDBname, String keyName, String valueName, Ehcache ehcache) {

        String key = schemaAndDBname + keyName + valueName;
        Element element;
        LOG.info("Get data for key: " + key);
        
        element = ehcache.get(key);
        
        if (element == null) {
            
            LOG.info("Not found cache for key: " + key);
            Map<String, String> data = grabberDao.
                    getData(schemaAndDBname, keyName, valueName);

            if (data == null) {
                LOG.debug("Not found data in database for key: " + key);
                return null;
            }
            
            element = new Element(key, clientGson.toJson(data));
            ehcache.put(element);
            LOG.info("Added data to cache for key: " + key);
            
        }

        LOG.info("Get cache for key: " + key);
        return element;
    }
}
