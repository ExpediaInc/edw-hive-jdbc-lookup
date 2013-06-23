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
package com.epam.cache.service;

import com.epam.cache.dao.GrabberDao;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

/**
 * @author Illya Yalovyy
 * @author Yaroslav Mykhaylov
 */
@Service
public class GrabberServiceImpl implements GrabberService {

    @Autowired
    private GrabberDao grabberDao;

    @Cacheable(value = "getData")
    @Override
    public Map<String, String> getData(String schemaAndDBname, String keyName, String valueName) {


        return grabberDao.getData(schemaAndDBname, keyName, valueName);
    }
}
