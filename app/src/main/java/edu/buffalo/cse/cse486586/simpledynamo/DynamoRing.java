package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;

import static edu.buffalo.cse.cse486586.simpledynamo.Util.genHash;

public class DynamoRing {
    private static final String TAG = DynamoRing.class.getName();

    // Number of replicas including the coordinator
    private static final int N = 3;

    // the actual ring, hashed and sorted using SHA1
    private static final List<String> RING;

    // Number of distinct nodes
    private static final List<String> NODES;

    // Pre-computed hashes
    private static final Map<String, String> NODES_HASH; // <port,  hash>
    private static final Map<String, String> HASH_NODE; // <hash,  port>

    private static final CopyOnWriteArraySet<String> OFFLINE_NODES = new CopyOnWriteArraySet<>();

    static {
        RING = new ArrayList<String>(5) {
            // NOTE: this is terrible!, violates interface contract.
            // TODO: Use a Ring-buffer.
            @Override
            public String get(int index) {
                return (index < 0) ? super.get(size() + index) : super.get(index % size());
            }

            @Override
            public String toString() {
                if (isEmpty()) {
                    return "[]";
                }

                StringBuilder buffer = new StringBuilder();
                buffer.append('[');
                Iterator<String> it = iterator();
                while (it.hasNext()) {
                    String next = it.next();
                    if (next != null) {
                        buffer.append(HASH_NODE.get(next));
                    }
                    if (it.hasNext()) {
                        buffer.append(" -> "); // pretty print
                    }
                }
                buffer.append(']');
                return buffer.toString();
            }
        };

        NODES = new ArrayList<>(5);
        NODES.add("11108");
        NODES.add("11112");
        NODES.add("11116");
        NODES.add("11120");
        NODES.add("11124");

        NODES_HASH = new HashMap<>(5);
        HASH_NODE = new HashMap<>(5);
        for (String node : NODES) {
            String hash = genHash(String.valueOf(Integer.valueOf(node) / 2));

            RING.add(hash);

            NODES_HASH.put(node, hash);
            HASH_NODE.put(hash, node);
        }

        Collections.sort(RING);
        Log.d(TAG, "Dynamo Ring: " + RING);
    }

    public static LinkedHashSet<String> preferenceListForKey(String key) {
        LinkedHashSet<String> s = new LinkedHashSet<>(N + 1);
        String coordinator = coordinatorForKey(key);
        s.add(coordinator);
        s.addAll(replicasForCoordinator(coordinator));
        return s;
    }

    public static LinkedHashSet<String> preferenceListForNode(String node) {
        LinkedHashSet<String> s = new LinkedHashSet<>(N);
        s.add(node);
        s.addAll(replicasForCoordinator(node));
        return s;
    }

    public static String coordinatorForKey(String key) {
        String keyHash = genHash(key);

        for (String nodeHash : RING) {
            if (keyHash.compareTo(nodeHash) <= 0) {
                return HASH_NODE.get(nodeHash);
            }
        }
        return HASH_NODE.get(RING.get(0));
    }

    public static List<String> replicasForCoordinator(String coordinator) {
        int coordinatorIndex = RING.indexOf(NODES_HASH.get(coordinator));
        List<String> replicas = new ArrayList<>(N - 1);
        for (int i = 1; i <= (N - 1); i++) {
            String node = HASH_NODE.get(RING.get(coordinatorIndex + i));
            replicas.add(node);
        }
        return replicas;
    }

    public static List<String> recoveryNodes(String port) {
        int index = RING.indexOf(NODES_HASH.get(port));
        List<String> nodes = new ArrayList<>(4);
        // add successors/replicas
        for (int i = 1; i <= (N - 1); i++) {
            String node = HASH_NODE.get(RING.get(index + i));
            nodes.add(node);
        }
        // add predecessors
        for (int i = (N - 1); i > 0; i--) {
            String node = HASH_NODE.get(RING.get(index - i));
            nodes.add(node);
        }
        Log.d(TAG, "Recovery nodes for: " + port + " : " + nodes);
        return nodes;
    }

    public static List<String> allOtherNodes(String me) { // all other nodes except this node
        List<String> others = new ArrayList<>(5);
        for (String node : NODES) {
            if (!node.equals(me)) {
                others.add(node);
            }
        }
        return others;
    }

    public static int liveNodeCount() {
        return NODES.size() - OFFLINE_NODES.size();
    }

    public static boolean isOffline(String node) {
        return OFFLINE_NODES.contains(node);
    }

    public static void markOnline(String node) {
        if (OFFLINE_NODES.contains(node)) {
            Log.e(TAG, "ONLINE: node :" + node);
            OFFLINE_NODES.remove(node);
        }
    }

    public static void markOffline(String node) {
        if (OFFLINE_NODES.add(node)) {
            Log.e(TAG, "OFFLINE: node :" + node);
        }
    }

    public static int liveNodeCount(List<String> nodes) {
        int online = 0;
        for (String node : nodes) {
            if (!isOffline(node)) {
                online++;
            }
        }
        return online;
    }
}
