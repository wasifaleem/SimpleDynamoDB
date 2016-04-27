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
    public static final int TIMEOUT = 4000;
    private static Dynamo INSTANCE = null;

    private static Map<UUID, Map<String, String>> REPLIES = new ConcurrentHashMap<>();
    private static Map<UUID, AtomicInteger> REPLIES_COUNT = new ConcurrentHashMap<>();
    private static Map<UUID, Semaphore> SESSIONS = new ConcurrentHashMap<>();

    private final Context context;
    private final SimpleDynamoDB db;
    private String myId;
    private String myPort;
    private static volatile UUID recoverySessionId;

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
            db.drop(); // TODO: ensure correctness in replication, remove this later.

            // Start recovery
            Payload recoverRequest = track(Payload.recoverRequest(myPort));
            List<String> recoveryNodes = DynamoRing.recoveryNodes(myPort);
            sendTo(recoveryNodes, recoverRequest);
            recoverySessionId = recoverRequest.getSessionId();

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
                        Payload replicaInsert = track(payload.fromPort(myPort).nodeType(REPLICA), 2);
                        sendTo(replicas, replicaInsert);

                        // wait for ACK
                        if (waitForCompletion(replicaInsert.getSessionId(), 1000, TimeUnit.MILLISECONDS)) {
                            Log.d(TAG, "Received INSERT ACK from Replicas");
                        } else {
                            Log.w(TAG, "Replica INSERT TimedOut " + replicaInsert);
                        }
                        break;
                    }
                    case REPLICA: {
                        // Send ack.
                        sendAck(payload);

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
                waitForRecovery();
                switch (payload.getKey()) {
                    case ALL: {
                        Log.d(TAG, "QUERY REQUEST ALL " + payload);

                        Payload queryReply = payload.fromPort(myPort).messageType(Payload.MessageType.QUERY_REPLY);
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

                        switch (payload.getNodeType()) {
                            case COORDINATOR: {
                                Payload replicaQuery = track(Payload.queryRequest(myPort, payload.getKey(), REPLICA), 2);

                                List<String> replicas = DynamoRing.replicasForCoordinator(myPort);
                                sendTo(replicas, replicaQuery);

                                // wait for reply from replica's
                                if (waitForCompletion(replicaQuery.getSessionId(), 1000, TimeUnit.MILLISECONDS)) {
                                    Log.d(TAG, "Received Query Replies from Replica's");
                                } else {
                                    Log.d(TAG, "TimedOut while waiting for Query Replies from Replica's");
                                }

                                Payload queryReply = payload.fromPort(myPort).messageType(Payload.MessageType.QUERY_REPLY);

                                for (Map.Entry<String, String> entry : REPLIES.get(replicaQuery.getSessionId()).entrySet()) {
                                    queryReply.getQueryResults().put(entry.getKey(), entry.getValue());
                                }

                                Map<String, String> localResults = new HashMap<>();
                                try (Cursor cursor = db.query(payload.getKey())) {
                                    while (cursor.moveToNext()) {
                                        localResults.put(cursor.getString(0), cursor.getString(1));
                                    }
                                }
                                queryReply.getQueryResults().putAll(localResults);

                                if (!queryReply.getQueryResults().isEmpty()) {
                                    sendTo(payload.getFromPort(), queryReply);
                                } else {
                                    Log.e(TAG, "COORDINATOR Watch out for inconsistency!");
                                }

                                if (localResults.isEmpty()) { // resolve local inconsistency from replicas
                                    if (!REPLIES.get(replicaQuery.getSessionId()).isEmpty()) {
                                        for (Map.Entry<String, String> entry : REPLIES.get(replicaQuery.getSessionId()).entrySet()) {
                                            dbInsert(entry);
                                        }
                                    }
                                }

                                break;
                            }
                            default: {
                                Payload queryReply = payload.fromPort(myPort).messageType(Payload.MessageType.QUERY_REPLY);
                                try (Cursor cursor = db.query(payload.getKey())) {
                                    while (cursor.moveToNext()) {
                                        queryReply.getQueryResults().put(cursor.getString(0), cursor.getString(1));
                                    }
                                }
                                if (queryReply.getQueryResults().isEmpty()) {
                                    Log.e(TAG, payload.getNodeType() + " Watch out for inconsistency!");
                                } else {
                                    sendTo(payload.getFromPort(), queryReply);
                                }

                                break;
                            }
                        }

                        break;
                    }
                }
                break;
            }
            case QUERY_REPLY: {
                Log.d(TAG, "QUERY REPLY:" + payload);

                REPLIES.get(payload.getSessionId()).putAll(payload.getQueryResults());
                REPLIES_COUNT.get(payload.getSessionId()).getAndIncrement();

                switch (payload.getNodeType()) {
                    case COORDINATOR: {
                        Log.d(TAG, "QUERY REPLY FROM COORDINATOR: COMPLETED " + payload);
                        SESSIONS.get(payload.getSessionId()).release();
                        break;
                    }
                    case REPLICA: {
                        Log.d(TAG, "QUERY REPLY FROM REPLICA: " + payload);
                        SESSIONS.get(payload.getSessionId()).release();
                        break;
                    }
                    case ALL: {
                        if (REPLIES_COUNT.get(payload.getSessionId()).get()
                                == (DynamoRing.liveNodeCount(DynamoRing.allOtherNodes(myPort)) - 1)) {
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
                waitForRecovery();
                Log.d(TAG, "RECOVERY REQUEST from:" + payload.getFromPort() + ": " + payload);
                Payload recoveryReply = payload.fromPort(myPort).messageType(RECOVERY_REPLY);
                try (Cursor cursor = db.all()) {
                    while (cursor.moveToNext()) {
                        String key = cursor.getString(0);
                        if (DynamoRing.preferenceListForKey(key).contains(payload.getFromPort())) {
                            recoveryReply.getQueryResults().put(key, cursor.getString(1));
                        }
                    }
                }
                sendTo(payload.getFromPort(), recoveryReply);

                break;
            }
            case RECOVERY_REPLY: {
                Log.d(TAG, "RECOVERY REPLY from:" + payload.getFromPort() + ": " + payload);
                REPLIES_COUNT.get(payload.getSessionId()).getAndIncrement();
                for (Map.Entry<String, String> entry : payload.getQueryResults().entrySet()) {
                    ContentValues cv = new ContentValues(1);
                    cv.put("key", entry.getKey());
                    cv.put("value", entry.getValue());
                    db.insert(cv);
                }
                if (REPLIES_COUNT.get(payload.getSessionId()).get()
                        == (DynamoRing.liveNodeCount(DynamoRing.recoveryNodes(myPort)))) {
                    SESSIONS.get(payload.getSessionId()).release();
                    Log.d(TAG, "RECOVERY COMPLETED");
                }
                break;
            }
        }

    }

    public void insert(ContentValues values) {
        waitForRecovery();

        String key = values.getAsString("key");
        String value = values.getAsString("value");

        String coordinator = DynamoRing.coordinatorForKey(key);
        List<String> replicasForCoordinator = DynamoRing.replicasForCoordinator(coordinator);

        if (coordinator.equals(myPort)) {
            db.insert(values);
            Log.v(TAG, "Coordinator Inserted " + values.toString());

            Payload insert = track(Payload.insert(myPort, key, value, REPLICA), 2);
            sendTo(replicasForCoordinator, insert);

            Log.d(TAG, "Sending INSERT to replicas:" + replicasForCoordinator + " " + insert);

            // wait for ACK
            if (waitForCompletion(insert.getSessionId(), TIMEOUT, TimeUnit.MILLISECONDS)) {
                Log.d(TAG, "Received INSERT ACK from Replicas");
            } else {
                Log.w(TAG, "Replica INSERT TimedOut " + insert);
            }
        } else {
            if (replicasForCoordinator.contains(myPort)) {
                Log.v(TAG, "Replica Inserted " + values.toString());
                db.insert(values);
            }

            Payload insert = track(Payload.insert(myPort, key, value, COORDINATOR));

            sendTo(coordinator, insert);
            Log.d(TAG, "Sending INSERT to Coordinator:" + coordinator + " " + insert);

            // wait for ACK
            if (waitForCompletion(insert.getSessionId(), TIMEOUT, TimeUnit.MILLISECONDS)) {
                Log.d(TAG, "Received INSERT ACK from Coordinator");
            } else {
                Log.d(TAG, "Coordinator INSERT TimedOut " + insert);
                // timed out, i.e coordinator is down
                // forward to replicas.

                Payload replicaInsert = track(Payload.insert(myPort, key, value, REPLICA), 2);

                sendTo(replicasForCoordinator, replicaInsert);
                Log.d(TAG, "Forward INSERT to replicas " + replicasForCoordinator + " " + replicaInsert);

                // wait for ACK
                if (waitForCompletion(replicaInsert.getSessionId(), TIMEOUT, TimeUnit.MILLISECONDS)) {
                    Log.d(TAG, "Received INSERT ACK from Replicas");
                } else {
                    Log.w(TAG, "Replica INSERT TimedOut " + replicaInsert);
                }
            }
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
                    if (waitForCompletion(delete.getSessionId(), TIMEOUT, TimeUnit.MILLISECONDS)) {
                        Log.d(TAG, "Received DELETE ACK from Coordinator");
                    } else {
                        // timed out, i.e coordinator is down
                        // forward to replicas.

                        List<String> replicas = DynamoRing.replicasForCoordinator(coordinator);
                        sendTo(replicas, delete.nodeType(REPLICA));
                    }
                }
                break;
            }
        }
        return 0L;
    }

    public Cursor query(String key) {
        waitForRecovery();

        switch (key) {
            case ALL: {
                Log.d(TAG, "QUERY ALL: " + key);
                MatrixCursor result = new MatrixCursor(new String[]{"key", "value"});

                Payload query = track(Payload.queryRequest(myPort, ALL, Payload.NodeType.ALL));

                try (Cursor cursor = db.all()) {
                    while (cursor.moveToNext()) {
                        result.addRow(new String[]{cursor.getString(0), cursor.getString(1)});
                    }
                }

                sendTo(DynamoRing.allOtherNodes(myPort), query);

                if (waitForCompletion(query.getSessionId(), 4, TimeUnit.SECONDS)) {
                    for (Map.Entry<String, String> entry : REPLIES.get(query.getSessionId()).entrySet()) {
                        result.addRow(new String[]{entry.getKey(), entry.getValue()});
                    }
                    Log.d(TAG, "Query ALL completed");
                } else {
                    Log.w(TAG, "Timed out while querying ALL");
                }

                return result;
            }
            case LOCAL: {
                Log.d(TAG, "QUERY @ LOCAL");
                return db.all();
            }
            default: {
                String coordinator = DynamoRing.coordinatorForKey(key);
                List<String> replicasForCoordinator = DynamoRing.replicasForCoordinator(coordinator);
                boolean isReplica = (replicasForCoordinator.contains(myPort));
                if (coordinator.equals(myPort)) {
                    Log.d(TAG, "QUERY for key " + key);

                    MatrixCursor result = new MatrixCursor(new String[]{"key", "value"});
                    Payload replicaQuery = track(Payload.queryRequest(myPort, key, REPLICA), 2);

                    sendTo(replicasForCoordinator, replicaQuery);

                    // wait for reply from replica's
                    if (waitForCompletion(replicaQuery.getSessionId(), TIMEOUT, TimeUnit.MILLISECONDS)) {
                        Log.d(TAG, "Received Query Replies from Replica's");
                    } else {
                        Log.d(TAG, "TimedOut while waiting for Query Replies from Replica's");
                    }

                    for (Map.Entry<String, String> entry : REPLIES.get(replicaQuery.getSessionId()).entrySet()) {
                        result.addRow(new String[]{entry.getKey(), entry.getValue()});
                    }

                    Map<String, String> localResults = new HashMap<>();
                    try (Cursor cursor = db.query(key)) {
                        while (cursor.moveToNext()) {
                            result.addRow(new String[]{cursor.getString(0), cursor.getString(1)});
                            localResults.put(cursor.getString(0), cursor.getString(1));
                        }
                    }

                    if (localResults.isEmpty()) {
                        if (!REPLIES.get(replicaQuery.getSessionId()).isEmpty()) {
                            for (Map.Entry<String, String> entry : REPLIES.get(replicaQuery.getSessionId()).entrySet()) {
                                dbInsert(entry);
                            }
                        }
                    }

                    return result;
                } else {
                    Payload query = track(Payload.queryRequest(myPort, key, COORDINATOR));

                    Log.d(TAG, "QUERY Coordinator for key " + key);

                    sendTo(coordinator, query);

                    // wait for query replies
                    if (waitForCompletion(query.getSessionId(), TIMEOUT, TimeUnit.MILLISECONDS)) {
                        Map<String, String> resultMap = REPLIES.get(query.getSessionId());
                        if (resultMap.isEmpty()) {
                            Log.w(TAG, "QUERY COORDINATOR, watch out for inconsistencies");
                        } else {
                            Log.d(TAG, "Received Query Replies from Coordinator" + resultMap);

                        }
                        MatrixCursor result = new MatrixCursor(new String[]{"key", "value"});

                        for (Map.Entry<String, String> entry : resultMap.entrySet()) {
                            result.addRow(new String[]{entry.getKey(), entry.getValue()});
                            if (isReplica) {
                                dbInsert(entry);
                            }
                        }

                        Log.d(TAG, "After Received Query Replies from Coordinator" + resultMap);

                        return result;
                    } else {
                        Log.d(TAG, "QUERY TIMED-OUT for coordinator: " + query);
                        // timed out, i.e coordinator is down
                        // forward to replicas.

                        MatrixCursor result = new MatrixCursor(new String[]{"key", "value"});
                        Payload replicaQuery = track(Payload.queryRequest(myPort, key, REPLICA), 2);

                        sendTo(replicasForCoordinator, replicaQuery);

                        // wait for reply from replica's
                        if (waitForCompletion(replicaQuery.getSessionId(), 4, TimeUnit.SECONDS)) {
                            Log.d(TAG, "Received Query Replies from Replica's");
                        } else {
                            Log.d(TAG, "TimedOut while waiting for Query Replies from Replica's");
                        }

                        for (Map.Entry<String, String> entry : REPLIES.get(replicaQuery.getSessionId()).entrySet()) {
                            result.addRow(new String[]{entry.getKey(), entry.getValue()});
                            if (isReplica) {
                                dbInsert(entry);
                            }
                        }

                        return result;
                    }
                }
            }
        }
    }

    private synchronized void waitForRecovery() {
        if (recoverySessionId != null) {
            Log.d(TAG, "WAITING FOR RECOVERY");
            if (waitForCompletion(recoverySessionId, 5, TimeUnit.SECONDS)) {
                Log.d(TAG, "RECOVERY COMPLETED");
            } else {
                Log.d(TAG, "RECOVERY TIMED-OUT");
            }
            recoverySessionId = null;
        }
    }

    public boolean waitForCompletion(UUID session, long time, TimeUnit timeUnit) {
        Log.d(TAG, "waitForCompletion " + session + " : " + time + " : " + timeUnit);
        try {
            if (SESSIONS.get(session).tryAcquire(time, timeUnit)) {
                return true;
            } else {
                Log.w(TAG, "Timed out while waiting for: " + session);
            }
        } catch (InterruptedException e) {
            Log.e(TAG, "Interrupted while waiting for: " + session);
        }
        return false;
    }


    private Payload track(Payload in, int count) {
        REPLIES_COUNT.put(in.getSessionId(), new AtomicInteger(0));
        SESSIONS.put(in.getSessionId(), new Semaphore(-1 * (count - 1), true));
        REPLIES.put(in.getSessionId(), new ConcurrentHashMap<String, String>(0));
        return in;
    }

    private Payload track(Payload in) {
        REPLIES_COUNT.put(in.getSessionId(), new AtomicInteger(0));
        SESSIONS.put(in.getSessionId(), new Semaphore(0, true));
        REPLIES.put(in.getSessionId(), new ConcurrentHashMap<String, String>(0));
        return in;
    }

    private void sendAck(Payload payload) {
        String fromPort = payload.getFromPort();
        Payload ack = payload.messageType(Payload.MessageType.ACK).fromPort(myPort);
        sendTo(fromPort, ack);
        Log.d(TAG, "Sent ACK to:" + fromPort + ": " + payload);
    }

    private void dbInsert(Map.Entry<String, String> entry) {
        ContentValues cv = new ContentValues(1);
        cv.put("key", entry.getKey());
        cv.put("value", entry.getValue());
        db.insert(cv);
        Log.d(TAG, "Inserted: " + cv);
    }

    private void dbInsert(Payload payload) {
        ContentValues cv = new ContentValues(1);
        cv.put("key", payload.getKey());
        cv.put("value", payload.getValue());
        db.insert(cv);
        Log.d(TAG, "Inserted: " + cv);
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
