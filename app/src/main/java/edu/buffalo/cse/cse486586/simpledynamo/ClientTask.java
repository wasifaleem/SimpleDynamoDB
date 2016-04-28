package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.AsyncTask;
import android.util.Log;
import android.util.Pair;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

public class ClientTask extends AsyncTask<Pair<String, Payload>, Void, Set<ClientTask.Result>> {
    private static final String TAG = ClientTask.class.getName();
    public static final int TIMEOUT = 500;

    @Override
    protected Set<ClientTask.Result> doInBackground(Pair<String, Payload>... payloads) { // <to, payload>
        Set<Result> results = new HashSet<>();

        for (Pair<String, Payload> payloadPair : payloads) {
            String node = payloadPair.first;

            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(node)), TIMEOUT);
                OutputStream out = socket.getOutputStream();
                String serialized = payloadPair.second.serialize();
                if (serialized != null) {
                    out.write(serialized.getBytes(StandardCharsets.UTF_8));
                    out.flush();
                    results.add(new Result(true, node));
                }
                out.close();
            } catch (Exception e) {
                results.add(new Result(false, node));
                Log.e(TAG, "ClientTask socket Exception" + " while sending to " + node, e); // offline?
            }
        }

        return results;
    }

    @Override
    protected void onPostExecute(Set<ClientTask.Result> results) {
        for (Result r : results) {
            if (r.success) {
                DynamoRing.markOnline(r.node);
            } else {
                DynamoRing.markOffline(r.node);
            }
        }
    }


    public static class Result {
        private boolean success;
        private String node;

        public Result(boolean success, String node) {
            this.success = success;
            this.node = node;
        }
    }
}