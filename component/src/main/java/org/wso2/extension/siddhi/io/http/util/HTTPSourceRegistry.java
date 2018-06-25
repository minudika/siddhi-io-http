/*
 *  Copyright (c) 2018 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.wso2.extension.siddhi.io.http.util;

import org.wso2.extension.siddhi.io.http.source.HttpResponseSource;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Result handler
 */
public class HTTPSourceRegistry {
    private static Map<ResponseSourceID, HttpResponseSource> responseSourceRegistry = new ConcurrentHashMap<>();

    // handle response sources
    public static HttpResponseSource getResponseSource(String sinkId, String statusCode) {
        return responseSourceRegistry.get(new ResponseSourceID(sinkId, statusCode));
    }

    public static void registerResponseSource(String sinkId, String statusCode, HttpResponseSource source) {
        responseSourceRegistry.put(new ResponseSourceID(sinkId, statusCode), source);
    }

    public static void removeResponseSource(String sinkId, String statusCode) {
        responseSourceRegistry.remove(new ResponseSourceID(sinkId, statusCode));
    }
}
