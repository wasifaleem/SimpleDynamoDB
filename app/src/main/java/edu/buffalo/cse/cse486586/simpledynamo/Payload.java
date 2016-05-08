package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class Payload {
    private static final String TAG = Payload.class.getName();

    private UUID sessionId = UUID.randomUUID();
    private MessageType messageType;
    private NodeType nodeType;

    private String fromPort;
    private String key;
    private String value;

    private Map<String, Value> queryResults = new HashMap<>();

    private boolean ack = false;
    private long version = 1;

    public Payload() {
    }

    public Payload(Payload copy) {
        this.sessionId = copy.sessionId;
        this.messageType = copy.messageType;
        this.nodeType = copy.nodeType;
        this.ack = copy.ack;
        this.fromPort = copy.fromPort;
        this.key = copy.key;
        this.value = copy.value;
        this.queryResults = copy.queryResults;
        this.version = copy.version;
    }

    public String serialize() {
        try {
            JSONObject jsonObject = new JSONObject()
                    .put("sessionId", sessionId)
                    .put("fromPort", fromPort)
                    .put("messageType", messageType)
                    .put("nodeType", nodeType)
                    .put("ack", ack)
                    .put("key", key)
                    .put("value", value)
                    .put("version", version);

            JSONObject object = new JSONObject();
            for (Map.Entry<String, Value> e : queryResults.entrySet()) {
                object.put(e.getKey(), new JSONObject().put("value", e.getValue().getValue()).put("version", e.getValue().getVersion()));
            }
            jsonObject.put("queryResults", object);

            String json = jsonObject.toString();
//            Log.d(TAG, "JSON serialized: " + json);
            return json;
        } catch (JSONException e) {
            Log.e(TAG, "Cannot serialize payload " + toString(), e);
        }
        return null;
    }

    public static Payload deserialize(String json) {
        try {
//            Log.d(TAG, "Deserialize json " + json);
            JSONObject jsonObject = new JSONObject(json);
            Payload payload = new Payload();
            if (!jsonObject.isNull("sessionId")) {
                payload.sessionId = UUID.fromString(jsonObject.getString("sessionId"));
            }

            if (!jsonObject.isNull("fromPort")) {
                payload.fromPort = jsonObject.getString("fromPort");
            }
            if (!jsonObject.isNull("messageType")) {
                payload.messageType = MessageType.valueOf(jsonObject.getString("messageType"));
            }
            if (!jsonObject.isNull("nodeType")) {
                payload.nodeType = NodeType.valueOf(jsonObject.getString("nodeType"));
            }
            if (!jsonObject.isNull("ack")) {
                payload.ack = jsonObject.getBoolean("ack");
            }
            if (!jsonObject.isNull("key")) {
                payload.key = jsonObject.getString("key");
            }
            if (!jsonObject.isNull("value")) {
                payload.value = jsonObject.getString("value");
            }
            if (!jsonObject.isNull("version")) {
                payload.version = jsonObject.getLong("version");
            }
            if (!jsonObject.isNull("queryResults")) {
                JSONObject queryResults = jsonObject.getJSONObject("queryResults");
                Iterator<String> keys = queryResults.keys();
                while (keys.hasNext()) {
                    String key = keys.next();
                    JSONObject value = queryResults.getJSONObject(key);
                    payload.queryResults.put(key, new Value(value.getString("value"), value.getLong("version")));
                }
            }
            return payload;
        } catch (JSONException e) {
            Log.e(TAG, "Cannot deserialize: " + json, e);
        }
        return null;
    }

    public Payload messageType(MessageType messageType) {
        Payload payload = new Payload(this);
        payload.messageType = messageType;
        return payload;
    }

    public Payload ack(boolean ack) {
        Payload payload = new Payload(this);
        payload.ack = ack;
        return payload;
    }

    public Payload version(long version) {
        Payload payload = new Payload(this);
        payload.version = version;
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
        COORDINATOR, REPLICA, UPDATE, ALL
    }

    public static Payload insert(String fromPort, String key, String value, NodeType nodeType, long version) {
        Payload payload = new Payload();
        payload.fromPort = fromPort;
        payload.key = key;
        payload.value = value;
        payload.messageType = MessageType.INSERT;
        payload.nodeType = nodeType;
        payload.version = version;
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

    public Map<String, Value> getQueryResults() {
        return queryResults;
    }

    public void setQueryResults(Map<String, Value> queryResults) {
        this.queryResults = queryResults;
    }

    public boolean isAck() {
        return ack;
    }

    public void setAck(boolean ack) {
        this.ack = ack;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Payload payload = (Payload) o;
        return ack == payload.ack &&
                version == payload.version &&
                Objects.equals(sessionId, payload.sessionId) &&
                messageType == payload.messageType &&
                nodeType == payload.nodeType &&
                Objects.equals(fromPort, payload.fromPort) &&
                Objects.equals(key, payload.key) &&
                Objects.equals(value, payload.value) &&
                Objects.equals(queryResults, payload.queryResults);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, messageType, nodeType, fromPort, key, value, queryResults, ack, version);
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
        sb.append(", ack=").append(ack);
        sb.append(", version=").append(version);
        sb.append('}');
        return sb.toString();
    }

    public static class Value {
        private String value;
        private long version;

        public Value(String value, long version) {
            this.value = value;
            this.version = version;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public long getVersion() {
            return version;
        }

        public void setVersion(long version) {
            this.version = version;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Value{");
            sb.append("value='").append(value).append('\'');
            sb.append(", version=").append(version);
            sb.append('}');
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Value value1 = (Value) o;
            return version == value1.version &&
                    Objects.equals(value, value1.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value, version);
        }
    }
}
