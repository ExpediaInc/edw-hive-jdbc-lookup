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
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;
import org.apache.hadoop.io.Text;

/**
 *
 * @author Illya Yalovyy
 * @author Yaroslav Mykhaylov
 */
@Description(name = "EXP_LOOKUP_STRING", value = "_FUNC_(dataSourceName,tableName,keyName,valueName,key)",
        extended = "Lookup values from the data source by key.")
public class GenericUDFLookupString extends GenericUDF {

    private static final int PAR_DATA_SOURCE_NAME = 0;
    private static final int PAR_TABLE_NAME = 1;
    private static final int PAR_KEY_NAME = 2;
    private static final int PAR_VALUE_NAME = 3;
    private static final int PAR_KEY = 4;
    private final CacheClientGson client = new CacheClientGson();
    private ObjectInspector keyOI;
    private Map<String, String> cache;
    private Text result = new Text();

    ;

    {
        client.setURL("http://cheledwhdc901.karmalab.net:8080/cache/service/get/");
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 5) {
            throw new UDFArgumentLengthException("EXP_LOOKUP_STRING expects exactly 5 argument.");
        }

        for (int i = 0; i < arguments.length - 1; i++) {
            ObjectInspector oi = arguments[i];
            if (!(oi instanceof WritableConstantStringObjectInspector)) {
                throw new UDFArgumentTypeException(i,
                        "constant STRING is expected as " + (i + 1) + " argument. '" + oi.toString() + " is found.");
            }

        }

        String dataSourceName = ((WritableConstantStringObjectInspector) arguments[PAR_DATA_SOURCE_NAME]).getWritableConstantValue().toString();
        String tableName = ((WritableConstantStringObjectInspector) arguments[PAR_TABLE_NAME]).getWritableConstantValue().toString();
        String keyName = ((WritableConstantStringObjectInspector) arguments[PAR_KEY_NAME]).getWritableConstantValue().toString();
        String valueName = ((WritableConstantStringObjectInspector) arguments[PAR_VALUE_NAME]).getWritableConstantValue().toString();

        keyOI = arguments[PAR_KEY];

        populateCache(dataSourceName, tableName, keyName, valueName);

        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    private String returnKey(GenericUDF.DeferredObject dos) throws UDFArgumentTypeException, HiveException {
        if (((PrimitiveObjectInspector) keyOI).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.LONG) {
            return String.valueOf(((LongObjectInspector) keyOI).getPrimitiveJavaObject(dos.get()));
        }
        if (((PrimitiveObjectInspector) keyOI).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT) {
            return String.valueOf(((IntObjectInspector) keyOI).getPrimitiveJavaObject(dos.get()));
        }
        if (((PrimitiveObjectInspector) keyOI).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            return ((StringObjectInspector) keyOI).getPrimitiveJavaObject(dos.get());
        }

        throw new UDFArgumentTypeException(PAR_KEY + 1,
                "Is input correct primitive? target field must be a (string, int, bigint); found "
                + ((PrimitiveObjectInspector) keyOI).getTypeName());
    }

    @Override
    public Object evaluate(GenericUDF.DeferredObject[] dos) throws HiveException {
        if (dos[PAR_KEY].get() == null) {
            return null;
        }

        String key = returnKey(dos[PAR_KEY]);
        String value = cache.get(key);

        if (value == null) {
            value = "";
        }

        result.set(value);

        return result;
    }

    @Override
    public String getDisplayString(String[] strings) {
        StringBuilder buf = new StringBuilder();
        buf.append("EXP_LOOKUP_STRING(");
        for (int i = 0; i < strings.length; i++) {
            String string = strings[i];
            if (i > 0) {
                buf.append(',');
            }
            buf.append(string);
        }
        buf.append(')');
        return buf.toString();
    }

    private void populateCache(String dataSourceName, String tableName,
            String keyName, String valueName) {
        cache = client.fetchData(tableName, keyName, valueName);
    }
}
