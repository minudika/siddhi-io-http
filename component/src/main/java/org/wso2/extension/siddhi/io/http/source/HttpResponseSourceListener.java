package org.wso2.extension.siddhi.io.http.source;

import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.transport.http.netty.contract.HttpConnectorListener;
import org.wso2.transport.http.netty.message.HTTPCarbonMessage;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HttpResponseSourceListener implements HttpConnectorListener {
    private HttpWorkerThread httpWorkerThread;
    private SourceEventListener sourceEventListener;
    private String sourceId;
    private String[] trpProperties;
    private ExecutorService executorService;

    public HttpResponseSourceListener(SourceEventListener sourceEventListener, String sourceId,
                                      String[] trpProperties) {
        this.sourceEventListener = sourceEventListener;
        this.sourceId = sourceId;
        this.trpProperties = trpProperties;
        this.executorService = executorService;
        // TODO: 14/6/18 let number of threads to be configured
        this.executorService = Executors.newFixedThreadPool(5);
    }

    @Override
    public void onMessage(HTTPCarbonMessage httpMessage) {
        httpWorkerThread = new HttpWorkerThread(httpMessage, sourceEventListener, sourceId, trpProperties);
        executorService.execute(httpWorkerThread);
    }

    @Override
    public void onError(Throwable throwable) {

    }
}
