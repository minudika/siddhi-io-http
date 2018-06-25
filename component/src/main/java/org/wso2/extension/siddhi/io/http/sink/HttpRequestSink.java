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
package org.wso2.extension.siddhi.io.http.sink;


import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultLastHttpContent;

import org.apache.log4j.Logger;
import org.wso2.carbon.messaging.Header;
import org.wso2.extension.siddhi.io.http.sink.util.HttpSinkUtil;
import org.wso2.extension.siddhi.io.http.source.HttpResponseMessageListener;
import org.wso2.extension.siddhi.io.http.util.HttpConstants;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.Option;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.transport.http.netty.contract.HttpResponseFuture;
import org.wso2.transport.http.netty.message.HTTPCarbonMessage;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * {@code HttpRequestSink} Handle the HTTP publishing tasks.
 */
@Extension(name = "http-request", namespace = "sink",
        description = "HTTP response sink is correlated with the " +
                "The HTTP request source, through a unique `source.id`, and it send a response to the HTTP request " +
                "source having the same `source.id`. The response message can be formatted in `text`, `XML` or `JSON` "
                + "and can be sent with appropriate headers.",
        parameters = {
                @Parameter(
                        name = "source.id",
                        description = "Identifier of the source.",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "message.id",
                        description = "Identifier of the message.",
                        dynamic = true,
                        type = {DataType.STRING}),
                @Parameter(
                        name = "headers",
                        description = "The headers that should be included as HTTP response headers. There can be any" +
                                " number of headers concatenated on following format. \"'header1:value1'," +
                                "'header2:value2'\" User can include content-type header if he/she need to have any " +
                                "specific type for payload. If not system get the mapping type as the content-Type " +
                                "header (ie.`@map(xml)`:`application/xml`, `@map(json)`:`application/json`, " +
                                "`@map(text)`:`plain/text`) and if user does not include any mapping type then system "
                                + "gets the `plain/text` as default Content-Type header. If user does not include " +
                                "Content-Length header then system calculate the bytes size of payload and include it" +
                                " as content-length header.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = " "),
        },
        examples = {
                @Example(syntax =
                        "@sink(type='http-response', source.id='sampleSourceId', message.id='{{messageId}}', "
                                + "headers=\"'content-type:json','content-length:94'\""
                                + "@map(type='json', @payload('{{payloadBody}}')))\n"
                                + "define stream FooStream (payloadBody String, messageId string, headers string);\n",
                        description =
                                "If it is json mapping expected input should be in following format for FooStream:\n"
                                        + "{\n"
                                        + "{\"events\":\n"
                                        + "    {\"event\":\n"
                                        + "        \"symbol\":WSO2,\n"
                                        + "        \"price\":55.6,\n"
                                        + "        \"volume\":100,\n"
                                        + "    }\n"
                                        + "},\n"
                                        + "0cf708b1-7eae-440b-a93e-e72f801b486a,\n"
                                        + "Content-Length:24#Content-Location:USA\n"
                                        + "}\n\n"
                                        + "Above event will generate response for the matching source message " +
                                        "as below.\n\n"
                                        + "~Output http event payload\n"
                                        + "{\"events\":\n"
                                        + "    {\"event\":\n"
                                        + "        \"symbol\":WSO2,\n"
                                        + "        \"price\":55.6,\n"
                                        + "        \"volume\":100,\n"
                                        + "    }\n"
                                        + "}\n\n"
                                        + "~Output http event headers\n"
                                        + "Content-Length:24,\n"
                                        + "Content-Location:'USA',\n"
                                        + "Content-Type:'application/json'\n"
                )}
)
public class HttpRequestSink extends HttpSink {

    private static final Logger log = Logger.getLogger(HttpRequestSink.class);
    private String sinkId;
    private boolean isDownloadEnabled;
    private StreamDefinition outputStreamDefinition;
    private Option destinationPathOption;

    @Override
    protected void init(StreamDefinition outputStreamDefinition, OptionHolder optionHolder,
                        ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        super.init(outputStreamDefinition, optionHolder, configReader, siddhiAppContext);
        this.sinkId = optionHolder.validateAndGetStaticValue(HttpConstants.SINK_ID);
        this.isDownloadEnabled = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(HttpConstants
                .DOWNLOAD_ENABLED, HttpConstants.DEFAULT_DOWNLOAD_ENABLED_VALUE));
        this.destinationPathOption = optionHolder.getOrCreateOption(HttpConstants.DESTINATION_PATH, null);
        if (isDownloadEnabled && destinationPathOption == null) {
            throw new SiddhiAppCreationException("Download path (download.path) must be provided when downloading is " +
                    "enabled.");
        }
        this.outputStreamDefinition = outputStreamDefinition;
    }


    /**
     * This method will be called when events need to be published via this sink
     *
     * @param payload        payload of the event based on the supported event class exported by the extensions
     * @param dynamicOptions holds the dynamic options of this sink and Use this object to obtain dynamic options.
     */
    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions) {
        if (!publisherURLOption.isStatic()) {
            initClientConnector(dynamicOptions);
        }
        String headers = httpHeaderOption.getValue(dynamicOptions);
        String httpMethod = HttpConstants.EMPTY_STRING.equals(httpMethodOption.getValue(dynamicOptions)) ?
                HttpConstants.METHOD_DEFAULT : httpMethodOption.getValue(dynamicOptions);
        List<Header> headersList = HttpSinkUtil.getHeaders(headers);
        String contentType = HttpSinkUtil.getContentType(mapType, headersList);
        String messageBody = getMessageBody(payload);
        HTTPCarbonMessage cMessage = createHttpCarbonMessage(httpMethod);
        cMessage = generateCarbonMessage(headersList, contentType, httpMethod, cMessage);
        cMessage.addHttpContent(new DefaultLastHttpContent(Unpooled.wrappedBuffer(messageBody
                .getBytes(Charset.defaultCharset()))));
                HttpResponseFuture httpResponseFuture = clientConnector.send(cMessage);
        HttpResponseMessageListener httpListener =
                new HttpResponseMessageListener(getTrpProperties(dynamicOptions), sinkId, isDownloadEnabled);
        httpResponseFuture.setHttpConnectorListener(httpListener);
    }

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[] {HttpConstants.HEADERS, HttpConstants.METHOD,
                HttpConstants.DESTINATION_PATH, HttpConstants.PUBLISHER_URL};
    }

    private Map<String, Object> getTrpProperties(DynamicOptions dynamicOptions) {
        Event event = dynamicOptions.getEvent();
        Object[] data = event.getData();
        List<Attribute> attributes = outputStreamDefinition.getAttributeList();
        Map<String, Object> trpProperties = new HashMap<>();
        for (int i = 0; i < attributes.size(); i++) {
            trpProperties.put(attributes.get(i).getName(), data[i]);
        }
        if (isDownloadEnabled) {
            trpProperties.put(HttpConstants.DESTINATION_PATH, destinationPathOption.getValue(dynamicOptions));
        }
        return trpProperties;
    }

    private Map<String, String> getDownloads(DynamicOptions dynamicOptions) {
        if (!isDownloadEnabled) {
            return null;
        }
        Event event = dynamicOptions.getEvent();
        Object[] data = event.getData();
        List<Attribute> attributes = outputStreamDefinition.getAttributeList();
        Map<String, String> downloads = new HashMap<>();

        downloads.put(HttpConstants.FILE_URL, (String) data[0]);
        downloads.put(HttpConstants.DESTINATION_PATH, (String) data[1]);

        return downloads;
    }
}
