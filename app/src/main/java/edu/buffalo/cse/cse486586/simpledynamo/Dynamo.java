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
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static edu.buffalo.cse.cse486586.simpledynamo.Payload.MessageType.RECOVERY_REPLY;
import static edu.buffalo.cse.cse486586.simpledynamo.Payload.NodeType.COORDINATOR;
import static edu.buffalo.cse.cse486586.simpledynamo.Payload.NodeType.REPLICA;
import static edu.buffalo.cse.cse486586.simpledynamo.Util.ALL;
import static edu.buffalo.cse.cse486586.simpledynamo.Util.LOCAL;

public class Dynamo {
    public static final int SERVER_PORT = 10000;
    private static final String TAG = Dynamo.class.getName();
    public static final int TIMEOUT = 1500; // ms
    private static Dynamo INSTANCE = null;

    // used to aggregate query replies
    private static Map<UUID, Map<String, Payload.Value>> REPLIES = new ConcurrentHashMap<>();
    // used to track request/response sessions and wait.
    private static Map<UUID, Semaphore> SESSIONS = new ConcurrentHashMap<>();
    private static AtomicLong version = new AtomicLong(1);
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

            if (db.count() > 0) {
                db.drop();

                // Start recovery
                Payload recoverRequest = track(Payload.recoverRequest(myPort), 4);
                List<String> recoveryNodes = DynamoRing.recoveryNodes(myPort);
                sendTo(recoveryNodes, recoverRequest);
                recoverySessionId = recoverRequest.getSessionId();
            }
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
                        // Insert
                        payload = payload.version(newVersionNumber(payload.getKey())).fromPort(myPort);
                        dbInsert(payload);
                        Log.v(TAG, "Coordinator Inserted " + payload.getValue());

                        // Forward to replicas, by using chain replication
                        List<String> replicas = DynamoRing.replicasForCoordinator(myPort);

                        Payload replicaInsert1 = payload.nodeType(REPLICA);
                        Payload replicaInsert2 = track(replicaInsert1.ack(true));
                        sendTo(replicas.get(1), replicaInsert2);
                        sendTo(replicas.get(0), replicaInsert1);

                        // wait for ACK from last replica. i.e chain replication
                        if (waitForCompletion(replicaInsert2.getSessionId(), TIMEOUT, TimeUnit.MILLISECONDS)) {
                            Log.d(TAG, "Received INSERT ACK from last replica");
                        } else {
                            Log.w(TAG, "Replica INSERT TimedOut " + replicaInsert2);
                        }
                        break;
                    }
                    default: {
                        if (payload.isAck()) {
                            // Send ack if requested.
                            sendAck(payload);
                        }
                        boolean fromCoordinator = DynamoRing.coordinatorForKey(payload.getKey()).equals(payload.getFromPort());
                        if (!fromCoordinator && payload.getNodeType() == REPLICA) {
                            payload = payload.version(newVersionNumber(payload.getKey()));
                        }

                        // Insert
                        dbInsert(payload);
                        Log.v(TAG, "Replica Inserted " + payload);
                        break;
                    }
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
                        break;
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
                                queryReply.getQueryResults().put(cursor.getString(0), new Payload.Value(cursor.getString(1), cursor.getLong(2)));
                            }
                        }
                        sendTo(payload.getFromPort(), queryReply);

                        break;
                    }
                    default: {
                        Log.d(TAG, "QUERY REQUEST Key " + payload);

                        Payload queryReply = payload.fromPort(myPort).messageType(Payload.MessageType.QUERY_REPLY);
                        try (Cursor cursor = db.query(payload.getKey())) {
                            while (cursor.moveToNext()) {
                                queryReply.getQueryResults().put(cursor.getString(0), new Payload.Value(cursor.getString(1), cursor.getLong(cursor.getColumnIndex("version"))));
                            }
                        }
                        if (queryReply.getQueryResults().isEmpty()) {
                            Log.w(TAG, payload.getNodeType() + " Watch out for inconsistencies!");
                        } else {
                            sendTo(payload.getFromPort(), queryReply);
                        }
                        break;
                    }
                }
                break;
            }
            case QUERY_REPLY: {
                Log.d(TAG, "QUERY REPLY:" + payload);
                REPLIES.get(payload.getSessionId()).putAll(payload.getQueryResults());
                SESSIONS.get(payload.getSessionId()).release();
                break;
            }
            case RECOVERY_REQUEST: {
                waitForRecovery();

                Log.d(TAG, "RECOVERY REQUEST from:" + payload.getFromPort() + ": " + payload);
                Payload recoveryReply = payload.fromPort(myPort).messageType(RECOVERY_REPLY);
                long start = System.nanoTime();
                try (Cursor cursor = db.all()) {
                    while (cursor.moveToNext()) {
                        String key = cursor.getString(0);
                        if (DynamoRing.preferenceListForKey(key).contains(payload.getFromPort())) {
                            recoveryReply.getQueryResults().put(key, new Payload.Value(cursor.getString(1), cursor.getLong(cursor.getColumnIndex("version"))));
                        }
                    }
                }
                sendTo(payload.getFromPort(), recoveryReply);

                Log.d(TAG, "RECOVERY REPLY TIME took: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

                break;
            }
            case RECOVERY_REPLY: {
                Log.d(TAG, "RECOVERY REPLY from:" + payload.getFromPort() + ": " + payload);
                for (Map.Entry<String, Payload.Value> entry : payload.getQueryResults().entrySet()) {
                    long version = Long.MIN_VALUE;
                    try (Cursor cursor = db.query(entry.getKey())) {
                        while (cursor.moveToNext()) {
                            version = cursor.getLong(cursor.getColumnIndex("version"));
                        }
                    }
                    if (entry.getValue().getVersion() >= version) {
                        dbInsert(entry);
                    }
                }
                SESSIONS.get(payload.getSessionId()).release();
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
        long version = newVersionNumber(key);

        if (coordinator.equals(myPort)) {
            values.put("version", version);
            db.insert(values);
            Log.v(TAG, "Coordinator Inserted " + values.toString());

            // now replicate, wait for ack from last replica/partition (chain replication)
            Payload replicaInsert1 = Payload.insert(myPort, key, value, REPLICA, version);
            Payload replicaInsert2 = track(replicaInsert1.ack(true));
            sendTo(replicasForCoordinator.get(1), replicaInsert2);
            sendTo(replicasForCoordinator.get(0), replicaInsert1);

            Log.d(TAG, "Sending INSERT to replicas:" + replicasForCoordinator + " " + replicaInsert1);

            // wait for ACK
            if (waitForCompletion(replicaInsert2.getSessionId(), TIMEOUT * 2, TimeUnit.MILLISECONDS)) {
                Log.d(TAG, "Received INSERT ACK from Replicas");
            } else {
                Log.w(TAG, "Replica INSERT TimedOut " + replicaInsert1);
            }
        } else {
            Payload insert = track(Payload.insert(myPort, key, value, COORDINATOR, version));

            sendTo(coordinator, insert);
            Log.d(TAG, "Sending INSERT to Coordinator:" + coordinator + " " + insert);

            // wait for ACK
            if (waitForCompletion(insert.getSessionId(), TIMEOUT * 2, TimeUnit.MILLISECONDS)) {
                Log.d(TAG, "Received INSERT ACK from Coordinator");
            } else {
                Log.d(TAG, "Coordinator INSERT TimedOut " + insert);
                // timed out, i.e coordinator is down
                // forward to replicas.

                Payload replicaInsert1 = Payload.insert(myPort, key, value, REPLICA, version);
                Payload replicaInsert2 = track(replicaInsert1.ack(true));

                sendTo(replicasForCoordinator.get(1), replicaInsert2);
                sendTo(replicasForCoordinator.get(0), replicaInsert1);

                Log.d(TAG, "Forward INSERT to replicas " + replicasForCoordinator + " " + replicaInsert1);

                // wait for ACK
                if (waitForCompletion(replicaInsert2.getSessionId(), TIMEOUT * 2, TimeUnit.MILLISECONDS)) {
                    Log.d(TAG, "Received INSERT ACK from Replicas");
                } else {
                    Log.e(TAG, "Replica INSERT TimedOut " + replicaInsert1);

                    // try to send again to coordinator:
                    sendTo(coordinator, insert);
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

                Payload query = track(Payload.queryRequest(myPort, ALL, Payload.NodeType.ALL), 4);

                try (Cursor cursor = db.all()) {
                    while (cursor.moveToNext()) {
                        result.addRow(new String[]{cursor.getString(0), cursor.getString(1)});
                    }
                }

                sendTo(DynamoRing.allOtherNodes(myPort), query);

                if (waitForCompletion(query.getSessionId(), 10, TimeUnit.SECONDS)) {
                    Log.d(TAG, "Query ALL completed");
                } else {
                    Log.w(TAG, "Timed out while querying ALL");
                }

                for (Map.Entry<String, Payload.Value> entry : REPLIES.get(query.getSessionId()).entrySet()) {
                    result.addRow(new String[]{entry.getKey(), entry.getValue().getValue()});
                }

                return result;
            }
            case LOCAL: {
                Log.d(TAG, "QUERY @ LOCAL");
                MatrixCursor result = new MatrixCursor(new String[]{"key", "value"});
                try (Cursor cursor = db.all()) {
                    while (cursor.moveToNext()) {
                        result.addRow(new String[]{cursor.getString(0), cursor.getString(1)});
                    }
                }
                return result;
            }
            default: {
                Log.d(TAG, "QUERY for key " + key);

                MatrixCursor result = new MatrixCursor(new String[]{"key", "value"});

                String coordinator = DynamoRing.coordinatorForKey(key);
                List<String> replicas = DynamoRing.replicasForCoordinator(coordinator);

                Payload replicaQuery2 = track(Payload.queryRequest(myPort, key, REPLICA));
                sendTo(replicas.get(1), replicaQuery2);

                Payload coordinatorQuery = track(Payload.queryRequest(myPort, key, COORDINATOR));
                sendTo(coordinator, coordinatorQuery);

                Payload replicaQuery1 = track(Payload.queryRequest(myPort, key, REPLICA));
                sendTo(replicas.get(0), replicaQuery1);

                // wait for reply from responsible nodes
                if (waitForCompletion(replicaQuery2.getSessionId(), TIMEOUT, TimeUnit.MILLISECONDS)) {
                    Log.d(TAG, "Received last replica");
                } else {
                    Log.w(TAG, "TimedOut while waiting for Query Replies from last replica");
                }

                if (waitForCompletion(coordinatorQuery.getSessionId(), TIMEOUT, TimeUnit.MILLISECONDS)) {
                    Log.d(TAG, "Received Query Replies from Coordinator");
                } else {
                    Log.w(TAG, "TimedOut while waiting for Query Replies from Coordinator");
                }

                if (waitForCompletion(replicaQuery1.getSessionId(), TIMEOUT, TimeUnit.MILLISECONDS)) {
                    Log.d(TAG, "Received Query Replies from first replica");
                } else {
                    Log.w(TAG, "TimedOut while waiting from first replica");
                }

                Set<UUID> querySessions = new LinkedHashSet<>(3);
                querySessions.add(replicaQuery2.getSessionId());
                querySessions.add(coordinatorQuery.getSessionId());
                querySessions.add(replicaQuery1.getSessionId());

                Set<Payload.Value> valueSet = new HashSet<>(3);
                long latestVersion = Long.MIN_VALUE;
                String latestValue = "";

                for (UUID session : querySessions) {
                    for (Payload.Value v : REPLIES.get(session).values()) {
                        if (v.getVersion() > latestVersion) {
                            latestVersion = v.getVersion();
                            latestValue = v.getValue();
                        }
                        valueSet.add(v);
                    }
                }

                if (valueSet.size() > 1) {
                    Log.d(TAG, key + " Latest value: " + latestValue + " from: " + valueSet);
                }
                result.addRow(new String[]{key, latestValue});
                if (latestVersion > Long.MIN_VALUE) {
                    sendTo(DynamoRing.preferenceListForKey(key), Payload.insert(myPort, key, latestValue, Payload.NodeType.UPDATE, latestVersion));
                }
                return result;
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
            long start = System.nanoTime();
            if (SESSIONS.get(session).tryAcquire(time, timeUnit)) {
                Log.d(TAG, "waitForCompletion success took: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms.");
                return true;
            } else {
                Log.w(TAG, "Timed out while waiting for: " + session);
            }
        } catch (InterruptedException e) {
            Log.e(TAG, "Interrupted while waiting for: " + session);
        }
        return false;
    }


    private Payload track(Payload in, int replyCount) {
        SESSIONS.put(in.getSessionId(), new Semaphore(-1 * (replyCount - 1), true)); // replyCount->permits
        REPLIES.put(in.getSessionId(), new ConcurrentHashMap<String, Payload.Value>(0));
        return in;
    }

    private Payload track(Payload in) {
        SESSIONS.put(in.getSessionId(), new Semaphore(0, true));
        REPLIES.put(in.getSessionId(), new ConcurrentHashMap<String, Payload.Value>(0));
        return in;
    }

    private void sendAck(Payload payload) {
        String fromPort = payload.getFromPort();
        Payload ack = payload.messageType(Payload.MessageType.ACK).fromPort(myPort);
        sendTo(fromPort, ack);
        Log.d(TAG, "Sent ACK to:" + fromPort + ": " + payload);
    }

    private void dbInsert(Map.Entry<String, Payload.Value> entry) {
        ContentValues cv = new ContentValues(1);
        cv.put("key", entry.getKey());
        cv.put("value", entry.getValue().getValue());
        cv.put("version", entry.getValue().getVersion());
        db.insert(cv);
        Log.d(TAG, "Inserted: " + cv);
    }

    private void dbInsert(Payload payload) {
        ContentValues cv = new ContentValues(1);
        cv.put("key", payload.getKey());
        cv.put("value", payload.getValue());
        cv.put("version", payload.getVersion());
        db.insert(cv);
        Log.d(TAG, "Inserted: " + cv);
    }

    private long newVersionNumber(String key) {
        long versionL = 1;
        try (Cursor cursor = db.version(key)) {
            while (cursor.moveToNext()) {
                versionL = cursor.getLong(0) + 1;
                Log.d(TAG, "NEW VERSION: " + versionL + " " + key);
            }
        }
        return versionL;
    }

    private void sendTo(Collection<String> nodes, Payload p) {
        for (String node : nodes) {
            sendTo(node, p);
        }
    }

    private void sendTo(String node, Payload p) {
        Log.d(TAG, "Sending to: " + node + " : " + p);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new Pair<>(node, p));
    }
}
