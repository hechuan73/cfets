package provider;

import cn.com.cfets.data.InvalidDataException;
import cn.com.cfets.data.MetaObject;
import cn.com.cfets.imt.ConfigurationException;
import cn.com.cfets.imt.TransmitException;
import cn.com.cfets.imt.api.*;
import cn.com.cfets.imt.impl.DefaultImtMessage;
import cn.com.cfets.imt.impl.ultra.UltraImtContext;

import imix.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


public class BinaryAppProvider {

    private static final Logger log = LoggerFactory.getLogger(BinaryAppProvider.class);

    static class ImtSessionListenerImpl implements ImtSessionListener {

        @Override
        public void fromAdmin(ImtMessage message) {
            log.info("Receive admin message: " + message);
        }

        @Override
        public void fromApp(ImtMessage message) {
            log.debug("======================ImtSessionListenerImpl.fromApp " + message.getContext().getClass().getName() /*+ "--" + (message.getContext() instanceof ImtContext)*/);
            UltraImtContext context;
            if (message.getContext() instanceof UltraImtContext) {
                context = (UltraImtContext) message.getContext();
                log.info("from App receive request message requestid[" + context.getRequestID() +
                        "], request_method[" + context.getMethodName() + "], "
                        + "sessionMod[" + context.getSessionMode() + "],"
                        + "callbackListenerName[" + context.getCallbackListenerName() + "]");
                log.info("context user extend map:" + context.getUserExtendMap());
                log.info("context PushChannelIDs:" + context.getPushChannelIDs());
                log.info("context PushTargetsMap:" + context.getPushTargetsMap());
            } else {
                log.error("not application request");
                return;
            }

            switch (message.getType()) {
                case ImtMessage.TYPE_BINARY:
                    String content;
                    try {
                        content = new String((byte[]) message.getBody());
                        log.info("===================receive binary message:" + content);
                        context.setMethodName("onPrice");
                    } catch (InvalidDataException e2) {
                        e2.printStackTrace();
                    }

                    break;
                case ImtMessage.TYPE_IMIX_OBJECT:
                    try {
                        if (!(message.getBody() instanceof MetaObject)) {
                            log.error("receive message is not metaobject");

                        } else {
                            log.info("receive metaobject message");
                        }
                    } catch (InvalidDataException e1) {
                        log.error("metaobject convert error.");
                    }
                    break;
                case ImtMessage.TYPE_TEXT:
                    try {
                        log.info("receive text message:" + message.getBody().toString());
                    } catch (InvalidDataException e) {
                        log.error("object convert to string error");
                    }
                    break;
                case ImtMessage.TYPE_IMIX:
                    Message msg;
                    try {
                        msg = (Message) message.getBody();
                        log.info("receive imix message:" + msg.toString());
                    } catch (InvalidDataException e) {
                        log.error("imix convert error.");
                    }
                    break;
            }


            try {
                context = (UltraImtContext) message.getContext();

                UltraImtContext responseContext = UltraImtContext.CreatePushContext();
                HashMap<String, String> rcMap = new HashMap<>();
                rcMap.put("traceId", context.getUserExtendMap().get("traceId"));
                rcMap.put("spanId", UUID.randomUUID().toString());
                responseContext.setUserExtendMap(rcMap);

                ImtMessage responseMsg = new DefaultImtMessage();
                responseMsg.setType(ImtMessage.TYPE_BINARY);
                responseMsg.setContext(responseContext);
                responseMsg.setBody("Test message for basic response".getBytes());
                message.getImtSession().response(responseMsg);

                //push test
                UltraImtContext pushContext = UltraImtContext.CreatePushContext();
                pushContext.setMethodName("push_method");

                List<String> pushTargets = new ArrayList<String>();
                pushTargets.add("PushTarget1");
                pushTargets.add("PushTarget2");
                pushTargets.add(null);
                Map<String, List<String>> pushTargetsMap = new HashMap<String, List<String>>();
                pushTargetsMap.put("test", pushTargets);
                pushTargetsMap.put("test2", null);
                pushContext.setPushTargetsMap(pushTargetsMap);
                pushContext.setPushChannelIDs(pushTargets);

                HashMap<String, String> userExtendMap = new HashMap<>();
                rcMap.put("traceId", context.getUserExtendMap().get("traceId"));
                rcMap.put("spanId", UUID.randomUUID().toString());
                pushContext.setUserExtendMap(rcMap);

                log.info("<<<<<<<<<<" + pushContext.getPushChannelIDs().toString());

                ImtMessage pushMsg = new DefaultImtMessage();
                pushMsg.setType(ImtMessage.TYPE_BINARY);
                pushMsg.setContext(pushContext);
                pushMsg.setBody("Test message for push response".getBytes());
                message.getImtSession().push(pushMsg);

            } catch (ConfigurationException e) {
                log.error("response error: " + e.getMessage());

            } catch (TransmitException e) {
                log.error("response error: " + e.getMessage());

            } catch (InvalidDataException e) {
                log.error("response error: " + e.getMessage());
            }


        }

        @Override
        public void onEvent(ImtEvent event) {
            log.info("On session event: " + event.getEventType());
            switch (event.getEventType()) {
                case ImtEvent.SERVICE_SUBSCRIBED:
                    break;
                case ImtEvent.SERVICE_UNSUBSCRIBED:
                    break;
                case ImtEvent.SERVICE_REGISTRATION_COMPLETE:
                    break;
                case ImtEvent.SERVICE_DEREGISTRATION_COMPLETE:
                    break;
                default:
                    log.info("not solve session event[" + event.getEventType() + "]");
                    break;
            }
        }

        @Override
        public void onLogon() {
            log.info("OnLogon.");
        }

        @Override
        public void onLogout() {
            log.info("OnLogout");
        }

        @Override
        public void onLastLook(ImtMessage message) {
            log.info("OnLastLook: " + message);
        }

    }

    static class ImtClusterListenerImpl implements ImtClusterListener {

        @Override
        public void onEvent(ImtEvent event) {
            log.info("On event: " + event.getEventType());
            switch (event.getEventType()) {
                case ImtEvent.AUTHENTICATION_SUCCESS:

                    break;
                case ImtEvent.AUTHENTICATION_FAILED:

                    break;
                case ImtEvent.CLUSTER_CLIENT_GO_LIVE:

                    break;
                case ImtEvent.CLUSTER_CLIENT_CRASH:

                    break;
                default:
                    log.info("not solve event[" + event.getEventType() + "]");
            }
        }

        @Override
        public void onException(Exception exception) {
            log.error("OnException: ", exception);
        }

    }

    public static void main(String[] args) throws ConfigurationException, TransmitException, InterruptedException, IOException {
        String log4jConfigPath = args[0];
        String imtConfigPath = args[1];
        String serviceID = args[2];


        org.apache.log4j.PropertyConfigurator.configure(log4jConfigPath);
        ImtApplication application = new ImtApplication(imtConfigPath);
        application.setClusterListener(new ImtClusterListenerImpl());
        application.init();

        ImtSession session = application.getSession(serviceID);
        session.setSessionListener(new ImtSessionListenerImpl());
        session.start();

        Scanner scan = new Scanner(System.in);
        while (scan.hasNext()) {

            if ("q".equals(scan.nextLine())) {
                break;
            }
        }

        scan.next();
        scan.close();
        application.destroy();
    }
}
