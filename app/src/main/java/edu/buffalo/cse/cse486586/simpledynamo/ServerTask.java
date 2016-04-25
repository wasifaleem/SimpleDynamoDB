package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

class ServerTask extends AsyncTask<ServerSocket, String, Void> {
    private static final String TAG = ServerTask.class.getName();
    private final Context context;

    public ServerTask(Context context) {
        this.context = context;
    }

    @Override
    protected Void doInBackground(ServerSocket... sockets) {
        ServerSocket serverSocket = sockets[0];
        while (true) {
            try (Socket socket = serverSocket.accept();
                 InputStream in = socket.getInputStream()) {

                ObjectInputStream objectInputStream = new ObjectInputStream(in);
                Payload payload = (Payload) objectInputStream.readObject();

                Dynamo.get(context).handle(payload);
            } catch (UnknownHostException e) {
                Log.e(TAG, "ServerTask UnknownHostException", e);
            } catch (IOException e) {
                Log.e(TAG, "ServerTask socket IOException", e);
            } catch (ClassNotFoundException e) {
                Log.e(TAG, "ServerTask socket ClassNotFoundException", e);
            } catch (NullPointerException e) {
                Log.e(TAG, "ServerTask NullPointerException", e);
            } catch (Exception e) {
                Log.e(TAG, "ServerTask Exception", e);
            }
        }
    }
}