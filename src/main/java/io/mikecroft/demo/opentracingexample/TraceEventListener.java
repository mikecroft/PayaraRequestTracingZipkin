/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.mikecroft.demo.opentracingexample;

import java.io.StringReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonPointer;
import org.apache.commons.lang3.StringUtils;
import zipkin2.Span;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.urlconnection.URLConnectionSender;

/**
 *
 * @author mike
 */
@MessageDriven(activationConfig = {
    @ActivationConfigProperty(propertyName = "destinationLookup", propertyValue = "jms/notifierQueue")
    ,
    @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue")
    ,
    @ActivationConfigProperty(propertyName = "destination", propertyValue = "notifierQueue")})
public class TraceEventListener implements MessageListener {

    @Override
    public void onMessage(Message message) {
        try {
            String msg = message.getBody(String.class);
            System.out.println("Message received: " + message.getBody(String.class));

            if (msg.contains("traceSpans")) {
                msg = StringUtils.substringAfter(msg, "\n");

                JsonObject trace = Json.createReader(new StringReader(msg)).readObject();
                JsonArray spanArray = trace.getJsonArray("traceSpans");
                AsyncReporter<Span> reporter = AsyncReporter.create(URLConnectionSender.create("http://localhost:9411/api/v2/spans"));
                for (JsonObject o : spanArray.getValuesAs(JsonObject.class)) {
                    System.out.println("Span parsed: " + o.toString());
                    Span s = Span.newBuilder()
                            .traceId(StringUtils.remove(
                                    StringUtils.substring(o.getValue("/spanContext/traceId").toString(),1 ,32), "-"))
                            .parentId("6b221d5bc9e6496c")
                            .id(StringUtils.remove(
                                    StringUtils.substring(o.getValue("/spanContext/spanId").toString().toString(),1 ,16), "-"))
                            .name(o.getValue("/operationName").toString())
//                            .duration(Long.parseLong(o.getValue("/traceDuration").toString()))
                            .build();
                    reporter.report(s);

                }
            }

        } catch (JMSException ex) {
            Logger.getLogger(TraceEventListener.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
