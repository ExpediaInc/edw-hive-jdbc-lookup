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
package com.expedia.edw.cache.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

/**
 * @author Illya Yalovyy
 * @author Yaroslav Mykhaylov
 */
@Repository
public class GrabberDaoJDBC implements GrabberDao {

    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(GrabberDaoJDBC.class);
    @Autowired
    private DataSource dataSource;

    @Override
    public Map<String, String> getData(String schemaAndDBname,
            String keyName, String valueName) {
        Connection conn = null;
        MapperDictionary mapperDictionary = new MapperDictionary();
        Map<String, String> dictionary = null;
        PreparedStatement ps = null;
        try {
            conn = dataSource.getConnection();
            ps = conn.prepareStatement("SELECT " + keyName + ","
                    + valueName + "  FROM " + schemaAndDBname);
            dictionary = mapperDictionary.convert(ps.executeQuery());
            ps.close();

        } catch (SQLException e) {
            logger.error(e);
        } finally {

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.error(e);
                }

            }
        }
        return dictionary;
    }
}
