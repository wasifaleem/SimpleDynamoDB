package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StreamCorruptedException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

class ServerTask extends AsyncTask<ServerSocket, StringBuilder, Void> {
    private static final String TAG = ServerTask.class.getName();
    private final Context context;
    public static final int TIMEOUT = 300;

    public ServerTask(Context context) {
        this.context = context;
    }

    @Override
    protected Void doInBackground(ServerSocket... sockets) {
        ServerSocket serverSocket = sockets[0];
        int port = 0;
        while (true) {
            try (Socket socket = serverSocket.accept();
                 BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                port = socket.getPort();
                socket.setSoTimeout(TIMEOUT);
                StringBuilder total = new StringBuilder(200);
                String line;
                while ((line = br.readLine()) != null) {
                    total.append(line);
                }

                publishProgress(total);

            } catch (UnknownHostException e) {
                Log.e(TAG, "ServerTask UnknownHostException", e);
            } catch (NullPointerException e) {
                Log.e(TAG, "ServerTask NullPointerException", e);
            } catch (SocketTimeoutException | StreamCorruptedException | EOFException e) {
                Log.e(TAG, "ServerTask socket timeout at: " + port, e); // TODO: mark node offline?
            } catch (Exception e) {
                Log.e(TAG, "ServerTask Exception", e);
            }
        }
    }

    @Override
    protected void onProgressUpdate(StringBuilder... values) {
        try {
            for (final StringBuilder payloadString : values) {
                String json = payloadString.toString();
                if (!json.isEmpty()) {
                    final Payload payload = Payload.deserialize(json);
                    if (payload != null) {
                        Dynamo.get(context).handle(payload);
                    }
                }
            }
        } catch (Exception e) {
            Log.e(TAG, "ServerTask onProgressUpdate Exception", e);
        }
    }
}