package consumer;


import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;
import java.util.UUID;

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


public class BinaryAppConsumer {

    private static final Logger log = LoggerFactory.getLogger(BinaryAppConsumer.class);

    private static class ImtSessionListenerImpl implements ImtSessionListener {

        private int count = 0;

        @Override
        public void fromAdmin(ImtMessage message) {
            log.info("Receive admin message: " + message);
        }

        @Override
        public void fromApp(ImtMessage message) {
            UltraImtContext context;
            Object contextobj = message.getContext();
            if(contextobj instanceof UltraImtContext){
                ++count;
                //Application Service request or response message.
                context = (UltraImtContext)message.getContext();
                log.info("========================from App receive request message requestid["+context.getRequestID()+
                        "], request_method[" + context.getMethodName() + "], "
                        + "sessionMod[" +  context.getSessionMode() + "]");

                log.info("context user extend map:" + context.getUserExtendMap());
                log.info("context PushChannelIDs:" + context.getPushChannelIDs());
                log.info("context PushTargetsMap:" + context.getPushTargetsMap());

            }else{
                log.error("not application request");
                return;
            }
            switch(message.getType()){
                case ImtMessage.TYPE_BINARY:
                    try {
                    String content = new String((byte[])message.getBody());
                    log.info("=====================receive async binary message,msg.length[" + ((byte[])message.getBody()).length + "] content[" + content+"]"
                    		+ " method_name[" + context.getMethodName() + "]");
                    } catch (InvalidDataException e) {
                    	log.error("binary convert to String error.");
                    	e.printStackTrace();
                    }
                    break;
                case ImtMessage.TYPE_IMIX_OBJECT:
                    try {
                        if(!(message.getBody() instanceof MetaObject)){
                            log.error("receive message is not metaobject");
                        }else{
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
                        msg = (Message)message.getBody();
                        log.info("receive imix message:"+ msg.toString());
                    } catch (InvalidDataException e) {
                        log.error("imix convert error.");
                    }
                    break;
            }
        }

        @Override
        public void onEvent(ImtEvent event) {
            log.info("On event: " + event.getEventType());

            switch(event.getEventType()){
                case ImtEvent.SERVICE_SUBSCRIBED:
                    break;
                case ImtEvent.SERVICE_UNSUBSCRIBED:
                    break;
                case ImtEvent.SERVICE_REGISTRATION_COMPLETE:
                    break;
                case ImtEvent.SERVICE_DEREGISTRATION_COMPLETE:
                    break;
                default:
                    log.info("not solve session event["+event.getEventType()+"]" );
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

    private static class ImtClusterListenerImpl implements ImtClusterListener {

        @Override
        public void onEvent(ImtEvent event) {
            log.info("On event: " + event.getEventType());
            switch(event.getEventType()){
                case ImtEvent.AUTHENTICATION_SUCCESS:
                    break;
                case ImtEvent.AUTHENTICATION_FAILED:
                    break;
                case ImtEvent.CLUSTER_CLIENT_GO_LIVE:
                    break;
                case ImtEvent.CLUSTER_CLIENT_CRASH:
                    break;
                default:
                    log.info("not solve event["+event.getEventType()+"]");
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
        String sessionID = args[2];
        String methodName = args[3];

        org.apache.log4j.PropertyConfigurator.configure(log4jConfigPath);

        ImtApplication application = new ImtApplication(imtConfigPath);
        application.setClusterListener(new ImtClusterListenerImpl());
        application.init();

        ImtSession session = application.getSession(sessionID);
        if(null == session){
            log.error("SessionID["  + sessionID + "] can not found!");
            return;
        }
        session.setSessionListener(new ImtSessionListenerImpl());
        session.start();

        try (Scanner scanner = new Scanner(System.in)) {
            String command = "";
            while (scanner.hasNext()) {

                command = scanner.nextLine();

                if ("q".equals(command)) {
                    break;
                }

                log.info("=====================input: " + methodName);

                ImtMessage msg = new DefaultImtMessage();
                msg.setType(ImtMessage.TYPE_BINARY);
                msg.setBody(command.getBytes());

                // for subscribe, so use Push Context
                UltraImtContext rc = UltraImtContext.CreatePushContext();
                rc.setMethodName(methodName);
                rc.setCallbackListenerName("listenername");
                HashMap<String, String> rcMap = new HashMap<String, String>();
                rcMap.put("traceId", UUID.randomUUID().toString());
                rcMap.put("spanId", UUID.randomUUID().toString());
                rc.setUserExtendMap(rcMap);
                msg.setContext(rc);
                rc.subscribePushChannelID("PushTarget1"); // set which channel to subscribe to
                session.request(msg);
                log.info("The subscribe request has sent to provider!");

                // for request, so use Request Context
                UltraImtContext ultar = UltraImtContext.CreateRequestContext();
                ultar.setNonBlocking(false); // use sync request
                ultar.setMethodName(methodName);
                ultar.setCallbackListenerName("listenername");
                rcMap.put("traceId", UUID.randomUUID().toString());
                rcMap.put("spanId", UUID.randomUUID().toString());
                ultar.setUserExtendMap(rcMap);
                msg.setContext(ultar);

                if (session.isRunning()) {
                    session.send(msg);
                    log.info("The basic request has sent to provider!");
                } else {
                    log.info("SessionID[" + session.getSessionID() + "] is not ready. msg is not send.");
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            application.destroy();
        }

    }
}
