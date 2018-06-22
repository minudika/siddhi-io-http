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
package org.wso2.extension.siddhi.io.http.source;

import org.wso2.siddhi.core.exception.SiddhiAppCreationException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@code HttpConnectorRegistry} The code is responsible for maintaining the all active connector listeners for
 * response source.
 */
class HttpResponseSourceConnectorRegistry {
    private static HttpResponseSourceConnectorRegistry instance = new HttpResponseSourceConnectorRegistry();
    private Map<String, HttpResponseConnectorListener> sourceListenersMap = new ConcurrentHashMap<>();

    protected HttpResponseSourceConnectorRegistry() {
    }

    /**
     * Get HttpResponseSourceConnectorRegistry instance.
     *
     * @return HttpResponseSourceConnectorRegistry instance
     */
    static HttpResponseSourceConnectorRegistry getInstance() {
        return instance;
    }


    /**
     * Get the source listener map.
     *
     * @return the source listener map
     */
    Map<String, HttpResponseConnectorListener> getSourceListenersMap() {
        return this.sourceListenersMap;
    }


    /**
     * Register new source listener.
     *
     *  @param sourceId   the source id.
     */
    void registerSourceListener(HttpResponseConnectorListener httpResponseSourceListener, String sourceId) {
        HttpResponseConnectorListener sourceListener =
                this.sourceListenersMap.putIfAbsent(sourceId, httpResponseSourceListener);
        if (sourceListener != null) {
            throw new SiddhiAppCreationException("There is a connection already established for the source :" +
                    sourceId);
        }
    }

    /**
     * Unregister the source listener.
     *
     * @param sinkId   the sink id of the source
     * @param siddhiAppName name of the siddhi app
     */
    void unregisterSourceListener(String sinkId, String siddhiAppName) {
        HttpResponseConnectorListener httpSourceListener = this.sourceListenersMap.get(sinkId);
        if (httpSourceListener != null && httpSourceListener.getSiddhiAppName().equals(siddhiAppName)) {
            sourceListenersMap.remove(sinkId);
            httpSourceListener.disconnect();
        }
    }
}
