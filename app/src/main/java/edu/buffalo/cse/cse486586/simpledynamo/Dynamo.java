package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.util.Pair;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static edu.buffalo.cse.cse486586.simpledynamo.Payload.MessageType.RECOVERY_REPLY;
import static edu.buffalo.cse.cse486586.simpledynamo.Payload.NodeType.COORDINATOR;
import static edu.buffalo.cse.cse486586.simpledynamo.Payload.NodeType.REPLICA;
import static edu.buffalo.cse.cse486586.simpledynamo.Util.ALL;
import static edu.buffalo.cse.cse486586.simpledynamo.Util.LOCAL;

public class Dynamo {
    public static final int SERVER_PORT = 10000;
    private static final String TAG = Dynamo.class.getName();
    public static final int TIMEOUT = 3000;
    private static Dynamo INSTANCE = null;

    private static Map<UUID, Map<String, String>> QUERY_REPLIES = new ConcurrentHashMap<>();
    private static Map<UUID, AtomicInteger> QUERY_REPLIES_COUNT = new ConcurrentHashMap<>();
    private static Map<UUID, Semaphore> SESSIONS = new ConcurrentHashMap<>();

    private final Context context;
    private final SimpleDynamoDB db;
    private String myId;
    private String myPort;

    private Dynamo(Context context) {
        this.context = context;
        this.db = SimpleDynamoDB.db(context);

        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        this.myId = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        this.myPort = String.valueOf((Integer.parseInt(myId) * 2));
    }

    public static Dynamo get(Context context) {
        if (INSTANCE == null) {
            synchronized (Dynamo.class) {
                INSTANCE = new Dynamo(context);
                INSTANCE.start();
                Log.w(TAG, "Service started: " + INSTANCE.myPort);
            }
        }
        return INSTANCE;
    }

    public void start() {
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            serverSocket.setReuseAddress(true);
            new ServerTask(context)
                    .executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            sendTo(DynamoRing.recoveryNodes(myPort), Payload.recoverRequest(myPort));
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }
    }

    public void handle(Payload payload) {
        switch (payload.getMessageType()) {
            case ACK: {
                Log.d(TAG, "Received ACK " + payload);
                SESSIONS.get(payload.getSessionId()).release();
                break;
            }
            case INSERT: {
                switch (payload.getNodeType()) {
                    case COORDINATOR: {
                        // Send ack.
                        sendAck(payload);

                        // Insert
                        dbInsert(payload);
                        Log.v(TAG, "Coordinator Inserted " + payload.getValue());

                        // Forward to replicas
                        List<String> replicas = DynamoRing.replicasForCoordinator(myPort);
                        sendTo(replicas, payload.fromPort(myPort).nodeType(REPLICA));
                        break;
                    }
                    case REPLICA: {
                        // Insert
                        dbInsert(payload);
                        Log.v(TAG, "Replica Inserted " + payload);
                        break;
                    }
                    case ALL: // no semantics for insert all.
                        break;
                }
                break;
            }
            case DELETE: {
                switch (payload.getNodeType()) {
                    case COORDINATOR: {
                        // Send ack.
                        sendAck(payload);

                        List<String> replicas = DynamoRing.replicasForCoordinator(myPort);
                        sendTo(replicas, payload.fromPort(myPort).nodeType(REPLICA));
                        break;
                    }
                    default:
                        break;
                }
                switch (payload.getKey()) {
                    case ALL: {
                        Log.d(TAG, "DELETE ALL: " + payload);
                        db.drop();
                        break;
                    }
                    default: {
                        Log.d(TAG, "DELETE : " + payload);
                        db.delete(payload.getKey());
                    }
                }
                break;
            }
            case QUERY_REQUEST: {
                Payload queryReply = payload.fromPort(myPort).messageType(Payload.MessageType.QUERY_REPLY);
                switch (payload.getKey()) {
                    case ALL: {
                        Log.d(TAG, "QUERY REQUEST ALL " + payload);


                        try (Cursor cursor = db.all()) {
                            while (cursor.moveToNext()) {
                                queryReply.getQueryResults().put(cursor.getString(0), cursor.getString(1));
                            }
                        }
                        sendTo(payload.getFromPort(), queryReply);

                        break;
                    }
                    default: {
                        Log.d(TAG, "QUERY REQUEST Key " + payload);

                        try (Cursor cursor = db.query(payload.getKey())) {
                            while (cursor.moveToNext()) {
                                queryReply.getQueryResults().put(cursor.getString(0), cursor.getString(1));
                            }
                        }
                        sendTo(payload.getFromPort(), queryReply);

                        break;
                    }
                }
                break;
            }
            case QUERY_REPLY: {
                Log.d(TAG, "QUERY REPLY:" + payload);
                if (QUERY_REPLIES.get(payload.getSessionId()) != null) {
                    QUERY_REPLIES.get(payload.getSessionId()).putAll(payload.getQueryResults());
                } else {
                    Log.d(TAG, "SHOULD this happen?:" + payload);
                }
                if (QUERY_REPLIES_COUNT.get(payload.getSessionId()) != null) {
                    QUERY_REPLIES_COUNT.get(payload.getSessionId()).getAndIncrement();
                } else {
                    Log.d(TAG, "SHOULD this happen?:" + payload);
                }
                switch (payload.getNodeType()) {
                    case COORDINATOR: {
                        Log.d(TAG, "QUERY REPLY FROM COORDINATOR: COMPLETED " + payload);
                        SESSIONS.get(payload.getSessionId()).release();
                        break;
                    }
                    case REPLICA: {
                        if (SESSIONS.get(payload.getSessionId()) != null) {
                            Log.d(TAG, "QUERY REPLY FROM REPLICA: " + payload);
                            SESSIONS.get(payload.getSessionId()).release();
                        } else {
                            Log.d(TAG, "QUERY REPLY FROM other REPLICA: " + payload);
                        }
                        break;
                    }
                    case ALL: {
                        if (QUERY_REPLIES_COUNT.get(payload.getSessionId()).get() == (DynamoRing.liveNodeCount() - 1)) {
                            SESSIONS.get(payload.getSessionId()).release();
                            Log.d(TAG, "QUERY REPLY FOR ALL COMPLETED: " + payload);
                        } else {
                            Log.d(TAG, "QUERY REPLY FOR ALL: " + payload);
                        }
                        break;
                    }
                }
                break;
            }
            case RECOVERY_REQUEST: {
                Log.d(TAG, "RECOVERY REQUEST from:" + payload.getFromPort() + ": " + payload);
                Payload recoveryReply = payload.fromPort(myPort).messageType(RECOVERY_REPLY);
                try (Cursor cursor = db.all()) {
                    while (cursor.moveToNext()) {
                        String key = cursor.getString(0);
                        recoveryReply.getQueryResults().put(key, cursor.getString(1));
                    }
                }
                sendTo(payload.getFromPort(), recoveryReply);

                break;
            }
            case RECOVERY_REPLY: {
                Log.d(TAG, "RECOVERY REPLY from:" + payload.getFromPort() + ": " + payload);
                for (Map.Entry<String, String> entry : payload.getQueryResults().entrySet()) {
                    ContentValues cv = new ContentValues(1);
                    cv.put("key", entry.getKey());
                    cv.put("value", entry.getValue());
                    db.insert(cv);
                }
                break;
            }
        }

    }

    private void sendAck(Payload payload) {
        String fromPort = payload.getFromPort();
        Payload ack = payload.messageType(Payload.MessageType.ACK).fromPort(myPort);
        Log.d(TAG, "Sending ACK to:" + fromPort + ": " + payload);
        sendTo(fromPort, ack);
    }

    private void dbInsert(Payload payload) {
        ContentValues cv = new ContentValues(1);
        cv.put("key", payload.getKey());
        cv.put("value", payload.getValue());
        db.insert(cv);
        Log.d(TAG, "Inserted: " + cv);
    }

    public void insert(ContentValues values) {
        String key = values.getAsString("key");
        String value = values.getAsString("value");

        String coordinator = DynamoRing.coordinatorForKey(key);

        if (coordinator.equals(myPort)) {
            db.insert(values);
            Log.v(TAG, "Coordinator Inserted " + values.toString());

            List<String> replicas = DynamoRing.replicasForCoordinator(coordinator);

            Payload insert = Payload.insert(myPort, key, value, REPLICA);
            sendTo(replicas, insert);
            Log.d(TAG, "Sending INSERT to replicas:" + replicas + " " + insert);
        } else {
            Payload insert = Payload.insert(myPort, key, value, COORDINATOR);
            SESSIONS.put(insert.getSessionId(), new Semaphore(0));

            sendTo(coordinator, insert); // TODO: check if coordinator is offline!
            Log.d(TAG, "Sending INSERT to Coordinator:" + coordinator + " " + insert);

            // wait for ACK
            try {
                if (SESSIONS.get(insert.getSessionId()).tryAcquire(TIMEOUT, TimeUnit.MILLISECONDS)) {
                    Log.d(TAG, "Received INSERT ACK from Coordinator");
                } else {
                    Log.d(TAG, "Coordinator INSERT TimedOut" + insert);
                    // timed out, i.e coordinator is down
                    // forward to replicas.

                    List<String> replicas = DynamoRing.replicasForCoordinator(coordinator);
                    sendTo(replicas, insert.nodeType(REPLICA));
                    Log.d(TAG, "Forward INSERT to replicas " + replicas + " " + insert);
                }
            } catch (InterruptedException e) {
                Log.e(TAG, "Interrupted while waiting for ACK", e);
            }
            SESSIONS.remove(insert.getSessionId());
        }
    }

    public long delete(String key) {
        switch (key) {
            case ALL: {
                db.drop();
                sendTo(DynamoRing.allOtherNodes(myPort), Payload.delete(myPort, ALL, Payload.NodeType.ALL));
                break;
            }
            case LOCAL: {
                return db.drop();
            }
            default: {
                String coordinator = DynamoRing.coordinatorForKey(key);

                if (coordinator.equals(myPort)) {
                    db.delete(key);

                    List<String> replicas = DynamoRing.replicasForCoordinator(coordinator);
                    sendTo(replicas, Payload.delete(myPort, key, REPLICA));
                } else {
                    Payload delete = Payload.delete(myPort, key, COORDINATOR);
                    SESSIONS.put(delete.getSessionId(), new Semaphore(0));

                    sendTo(coordinator, delete);

                    // wait for ACK
                    try {
                        if (SESSIONS.get(delete.getSessionId()).tryAcquire(TIMEOUT, TimeUnit.MILLISECONDS)) {
                            Log.d(TAG, "Received DELETE ACK from Coordinator");
                        } else {
                            // timed out, i.e coordinator is down
                            SESSIONS.remove(delete.getSessionId());
                            // forward to replicas.

                            List<String> replicas = DynamoRing.replicasForCoordinator(coordinator);
                            sendTo(replicas, delete.nodeType(REPLICA));
                        }
                    } catch (InterruptedException e) {
                        Log.e(TAG, "Interrupted while waiting for ACK", e);
                    }
                }
                break;
            }
        }
        return 0L;
    }

    public Cursor query(String key) {
        switch (key) {
            case ALL: {
                Log.d(TAG, "QUERY ALL: " + key);
                MatrixCursor result = new MatrixCursor(new String[]{"key", "value"});

                Payload query = queryRequest(ALL, Payload.NodeType.ALL);

                try (Cursor cursor = db.all()) {
                    while (cursor.moveToNext()) {
                        result.addRow(new String[]{cursor.getString(0), cursor.getString(1)});
                    }
                }

                sendTo(DynamoRing.allOtherNodes(myPort), query);

                SESSIONS.get(query.getSessionId()).acquireUninterruptibly();
                for (Map.Entry<String, String> entry : QUERY_REPLIES.get(query.getSessionId()).entrySet()) {
                    result.addRow(new String[]{entry.getKey(), entry.getValue()});
                }
                QUERY_REPLIES.remove(query.getSessionId());
                QUERY_REPLIES_COUNT.remove(query.getSessionId());
                SESSIONS.remove(query.getSessionId());

                return result;
            }
            case LOCAL: {
                Log.d(TAG, "QUERY for LOCAL");
                return db.all();
            }
            default: {
                String coordinator = DynamoRing.coordinatorForKey(key);
                if (coordinator.equals(myPort)) {
                    Log.d(TAG, "QUERY for key " + key);
                    return db.query(key);
                } else {
                    MatrixCursor result = new MatrixCursor(new String[]{"key", "value"});

                    Payload query = queryRequest(key, COORDINATOR);
                    Log.d(TAG, "QUERY Coordinator for key " + key);

                    sendTo(coordinator, query);

                    // wait for query replies
                    try {
                        if (SESSIONS.get(query.getSessionId()).tryAcquire(TIMEOUT, TimeUnit.MILLISECONDS)) {
                            Log.d(TAG, "Received Query Replies from Coordinator");
                        } else {
                            // timed out, i.e coordinator is down
                            // SESSIONS.remove(query.getSessionId());
                            // forward to replicas.

                            List<String> replicas = DynamoRing.replicasForCoordinator(coordinator);
                            sendTo(replicas, query.nodeType(REPLICA));

                            // wait for reply from replica's
                            SESSIONS.get(query.getSessionId()).acquireUninterruptibly();
                            Log.d(TAG, "Received Query Replies from Replica's");
                        }

                        for (Map.Entry<String, String> entry : QUERY_REPLIES.get(query.getSessionId()).entrySet()) {
                            result.addRow(new String[]{entry.getKey(), entry.getValue()});
                        }
                        QUERY_REPLIES.remove(query.getSessionId());
                        QUERY_REPLIES_COUNT.remove(query.getSessionId());
                        SESSIONS.remove(query.getSessionId());
                    } catch (InterruptedException e) {
                        Log.e(TAG, "Interrupted while waiting for ACK", e);
                    }
                    return result;
                }
            }
        }
    }

    private Payload queryRequest(String key, Payload.NodeType nodeType) {
        Payload query = Payload.queryRequest(myPort, key, nodeType);
        QUERY_REPLIES.put(query.getSessionId(), new HashMap<String, String>());
        QUERY_REPLIES_COUNT.put(query.getSessionId(), new AtomicInteger(0));
        SESSIONS.put(query.getSessionId(), new Semaphore(0));
        return query;
    }

    private void sendTo(List<String> nodes, Payload p) {
        Pair[] payloads = new Pair[nodes.size()];
        for (int i = 0, nodesSize = nodes.size(); i < nodesSize; i++) {
            payloads[i] = new Pair<>(nodes.get(i), p);
        }
        Log.d(TAG, "Sending to: " + nodes + " : " + p);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, payloads);
    }

    private void sendTo(String node, Payload p) {
        Log.d(TAG, "Sending to: " + node + " : " + p);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new Pair<>(node, p));
    }
}
