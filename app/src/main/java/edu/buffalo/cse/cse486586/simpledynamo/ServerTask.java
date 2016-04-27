package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.LinkedBlockingQueue;

class ServerTask extends AsyncTask<ServerSocket, StringBuilder, Void> {
    private static final String TAG = ServerTask.class.getName();
    private final Context context;
//    private final LinkedBlockingQueue<Payload> queue;

    public ServerTask(Context context) {
        this.context = context;
//        this.queue = new LinkedBlockingQueue<>();
//        new WorkerTask(this.queue).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
    }

    @Override
    protected Void doInBackground(ServerSocket... sockets) {
        ServerSocket serverSocket = sockets[0];
        while (true) {
            try (Socket socket = serverSocket.accept();
                 BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                StringBuilder total = new StringBuilder(200);
                String line;
                while ((line = br.readLine()) != null) {
                    total.append(line);
                }
//                if (payload != null) {
//                    queue.offer(payload);
//                    Dynamo.get(context).handle(payload);
                publishProgress(total);

//                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ServerTask UnknownHostException", e);
            } catch (IOException e) {
                Log.e(TAG, "ServerTask socket IOException", e);
            } catch (NullPointerException e) {
                Log.e(TAG, "ServerTask NullPointerException", e);
            } catch (Exception e) {
                Log.e(TAG, "ServerTask Exception", e);
            }
        }
    }

    @Override
    protected void onProgressUpdate(StringBuilder... values) {
        for (final StringBuilder payloadString : values) {
            final Payload payload = Payload.deserialize(payloadString.toString());
            if (payload != null) {
                Dynamo.get(context).handle(payload);
            }
        }
    }

//    public class WorkerTask extends AsyncTask<Void, Void, Void> {
//        private final LinkedBlockingQueue<Payload> queue;
//
//        public WorkerTask(LinkedBlockingQueue<Payload> queue) {
//            this.queue = queue;
//        }
//
//        @Override
//        protected Void doInBackground(Void... params) {
//            while (true) {
//                try {
//                    Payload payload = queue.take();
//                    if (payload != null) {
//                        Dynamo.get(context).handle(payload);
//                    }
//                } catch (InterruptedException e) {
//                    Log.e(TAG, "WorkerTask interrupted", e);
//                }
//            }
//        }
//    }
}