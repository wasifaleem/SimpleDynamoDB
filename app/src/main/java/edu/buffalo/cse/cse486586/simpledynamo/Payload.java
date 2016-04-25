package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class Payload implements Serializable {
    private static final String TAG = Payload.class.getName();
    private static final long serialVersionUID = -6229103345195763019L;

    private UUID sessionId = UUID.randomUUID();
    private MessageType messageType;
    private NodeType nodeType;

    private String fromPort;
    private String key;
    private String value;
    private Map<String, String> queryResults = new HashMap<>();

    public Payload() {
    }

    public Payload(Payload copy) {
        this.sessionId = copy.sessionId;
        this.messageType = copy.messageType;
        this.nodeType = copy.nodeType;
        this.fromPort = copy.fromPort;
        this.key = copy.key;
        this.value = copy.value;
        this.queryResults = copy.queryResults;
    }

//    public Payload sessionId(UUID sessionId) {
//        Payload payload = new Payload(this);
//        payload.sessionId = sessionId;
//        return payload;
//    }

    public Payload messageType(MessageType messageType) {
        Payload payload = new Payload(this);
        payload.messageType = messageType;
        return payload;
    }

    public Payload nodeType(NodeType nodeType) {
        Payload payload = new Payload(this);
        payload.nodeType = nodeType;
        return payload;
    }

    public Payload fromPort(String fromPort) {
        Payload payload = new Payload(this);
        payload.fromPort = fromPort;
        return payload;
    }

    enum MessageType {
        ACK,
        DELETE,
        INSERT,
        QUERY_REQUEST,
        QUERY_REPLY,
        RECOVERY_REQUEST,
        RECOVERY_REPLY
    }

    enum NodeType {
        COORDINATOR, REPLICA, ALL
    }

    public static Payload insert(String fromPort, String key, String value, NodeType nodeType) {
        Payload payload = new Payload();
        payload.fromPort = fromPort;
        payload.key = key;
        payload.value = value;
        payload.messageType = MessageType.INSERT;
        payload.nodeType = nodeType;
        return payload;
    }

    public static Payload ack(NodeType nodeType, String fromPort, UUID sessionId, String key) {
        Payload payload = new Payload();
        payload.sessionId = sessionId;
        payload.fromPort = fromPort;
        payload.key = key;
        payload.messageType = MessageType.ACK;
        payload.nodeType = nodeType;
        return payload;
    }

    public static Payload delete(String fromPort, String key, NodeType nodeType) {
        Payload payload = new Payload();
        payload.fromPort = fromPort;
        payload.key = key;
        payload.messageType = MessageType.DELETE;
        payload.nodeType = nodeType;
        return payload;
    }

    public static Payload queryRequest(String fromPort, String key, NodeType nodeType) {
        Payload payload = new Payload();
        payload.fromPort = fromPort;
        payload.key = key;
        payload.messageType = MessageType.QUERY_REQUEST;
        payload.nodeType = nodeType;
        return payload;
    }

    public static Payload queryReply(String fromPort, UUID queryId, NodeType nodeType) {
        Payload payload = new Payload();
        payload.fromPort = fromPort;
        payload.sessionId = queryId;
        payload.messageType = MessageType.QUERY_REPLY;
        payload.nodeType = nodeType;
        return payload;
    }

    public static Payload recoverRequest(String fromPort) {
        Payload payload = new Payload();
        payload.fromPort = fromPort;
        payload.messageType = MessageType.RECOVERY_REQUEST;
        return payload;
    }

    public UUID getSessionId() {
        return sessionId;
    }

    public void setSessionId(UUID sessionId) {
        this.sessionId = sessionId;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    public NodeType getNodeType() {
        return nodeType;
    }

    public void setNodeType(NodeType nodeType) {
        this.nodeType = nodeType;
    }

    public String getFromPort() {
        return fromPort;
    }

    public void setFromPort(String fromPort) {
        this.fromPort = fromPort;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Map<String, String> getQueryResults() {
        return queryResults;
    }

    public void setQueryResults(Map<String, String> queryResults) {
        this.queryResults = queryResults;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Payload payload = (Payload) o;
        return Objects.equals(sessionId, payload.sessionId) &&
                messageType == payload.messageType &&
                nodeType == payload.nodeType &&
                Objects.equals(fromPort, payload.fromPort) &&
                Objects.equals(key, payload.key) &&
                Objects.equals(value, payload.value) &&
                Objects.equals(queryResults, payload.queryResults);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, messageType, nodeType, fromPort, key, value, queryResults);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Payload{");
        sb.append("sessionId=").append(sessionId);
        sb.append(", messageType=").append(messageType);
        sb.append(", nodeType=").append(nodeType);
        sb.append(", fromPort='").append(fromPort).append('\'');
        sb.append(", key='").append(key).append('\'');
        sb.append(", value='").append(value).append('\'');
        sb.append(", queryResults=").append(queryResults);
        sb.append('}');
        return sb.toString();
    }
}
