package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {
    private String myPort;
    private  String nodeId;
    private String successor;
    private String predecessor;
    private String queryFound;
    private  String globalfound;
    private String queryall;
    HashMap<String, String> portId = new HashMap<String, String>();
    HashMap<String, String> joinMap = new HashMap<String, String>();
    ArrayList<String> joinList = new ArrayList<String>();



    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        String key = values.getAsString("key");
        String value = values.getAsString("value");
        Log.e("Key-value", "key:" + key +"," + "value:" + value);
        FindNode(key, value);


        /*Find the correct node id for the key to deliver */



        return null;
    }

    @Override
    public boolean onCreate() {

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);

        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.e("my port", myPort);
        portId.put("11108", "5554");
        portId.put("11112", "5556");
        portId.put("11116", "5558");
        portId.put("11120", "5560");
        portId.put("11124", "5562");


        /*Calculate Hash of myPort*/
        try {
            nodeId = genHash(portId.get(myPort));
            Log.e("Hash", nodeId);
            if (myPort.equals("11108")){
                predecessor = null;
                successor = null;
                joinList.add(nodeId);
                joinMap.put(nodeId, myPort);
                Log.e("AVD0", "hash added to list");
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        try {
            ServerSocket serverSocket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        }catch(IOException e){
            Log.e(TAG, "Can't create a ServerSocket");
            return false;

        }

        if (!myPort.equals("11108")){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "join-"+ myPort + "-" + nodeId);
        }


        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        queryFound = null;
        globalfound = null;
        queryall = null;
        Log.e("Selection", selection);
        Context context = getContext();
//        String ret = "";
//        String [] localFiles = context.fileList();
        Log.e("Local Files", context.fileList().toString());

            if (selection != null && selection.equals("@")){ //All Key-values at local AVD
                Cursor mat = localQuery();
                return  mat;
            }
            else if (selection != null && selection.equals("*")){
                if (predecessor == null && successor == null){
                    Cursor mat1 = localQuery();
                    return mat1;
                }else{
                    String globaldata = "";
                    globaldata = globalquery(globaldata);
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "globalquery--"+successor+"--"+globaldata+"--"+myPort+"--" + "false");
                    while (true){
                        if (globalfound == null){
                            Log.e("Global Waiting", "Key not found yet.");
                        }else{
                            break;
                        }
                    }
                    Cursor mat5 = parseGlobal(globalfound);
                    return  mat5;

                }
            }

            else if ( selection != null ) {
              if ((predecessor == null && successor == null)){
                MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
                String Q = SingleLocalQuery(selection);
                cursor.newRow()
                          .add("key", selection)
                          .add("value", Q);
                return  cursor;
              }
              else{
                  if (CanInsert(selection)){ //Query at Current AVD
                      MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
                      String query = SingleLocalQuery(selection);
                      cursor.newRow()
                              .add("key", selection)
                              .add("value", query);
                      return cursor;
                  }
                  else { //Route the Query to successor
                      new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "querysingle-"+successor+"-"+selection+"-"+myPort);
                      while (true){
                          if (queryFound==null){
                              Log.e("Waiting", "Key not found yet.");
                          }
                          else{
                              break;
                          }
                      }
                      MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
                      cursor.newRow()
                              .add("key", selection)
                              .add("value", queryFound);
                      return cursor;
                  }

              }
            } else if (selection.equals("*")){


            }

        return null;
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
    public Cursor localQuery() {
        Context context = getContext();
        String ret = "";
        String[] localFiles = context.fileList();
        MatrixCursor cursor = null;
        try {
            cursor = new MatrixCursor(new String[]{"key", "value"});
            for (int i = 0; i < localFiles.length; i++) {
                InputStream localInput = context.openFileInput(localFiles[i]);

                InputStreamReader inputStreamReader = new InputStreamReader(localInput);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                String receiveString = "";
                StringBuilder stringBuilder = new StringBuilder();

                while ((receiveString = bufferedReader.readLine()) != null) {
                    stringBuilder.append(receiveString);
                }

                localInput.close();
                ret = stringBuilder.toString();


                cursor.newRow()
                        .add("key", localFiles[i])
                        .add("value", ret);

                Log.v("Sel", localFiles[i]);
                Log.i("value", ret);
                Log.i("Cursor", String.valueOf(cursor));

            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return cursor;
    }

    public String globalquery(String data){
        Context context = getContext();
        String ret = "";
        String[] localFiles = context.fileList();
        for (int i = 0; i < localFiles.length; i++) {
            try{
                InputStream localInput = context.openFileInput(localFiles[i]);

                InputStreamReader inputStreamReader = new InputStreamReader(localInput);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                String receiveString = "";
                StringBuilder stringBuilder = new StringBuilder();

                while ((receiveString = bufferedReader.readLine()) != null) {
                    stringBuilder.append(receiveString);
                }

                localInput.close();
                ret = stringBuilder.toString();

                if (data.equals("")){
                    data = data + localFiles[i] + "-" + ret;
                }else {
                    data = data + "-" + localFiles[i] + "-" + ret;
                }


            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return data;

    }
    public Cursor parseGlobal(String gData){
        String [] d = gData.split("-");
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        for (int i = 0; i < d.length; i++){
            cursor.newRow()
                    .add("key", d[i])
                    .add("value", d[i+1]);
            i += 1;
        }

        return cursor;
    }
    public String SingleLocalQuery(String key) {

        Context context = getContext();
        String ret = "";
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        try {
            InputStream inputStream = context.openFileInput(key);
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String receiveString = "";
            StringBuilder stringBuilder = new StringBuilder();

            while ((receiveString = bufferedReader.readLine()) != null) {
                stringBuilder.append(receiveString);
            }

            inputStream.close();
            ret = stringBuilder.toString();




            Log.v("query", key);
            Log.i("value", ret);



        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }

    public void  ReturnOrForward(String inputQuery, String source){
        if (CanInsert(inputQuery)){
            String result = SingleLocalQuery(inputQuery);
            new  ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "foundquery-" + source + "-" + result );
        } else {
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "querysingle-"+successor+"-"+ inputQuery+"-"+source);
        }
    }

        private static ArrayList<String> LexoSort(ArrayList<String> words){
        int n = words.size();
        for(int i = 0; i < n-1; ++i) {
            for (int j = i + 1; j < n; ++j) {
                if (words.get(i).compareTo(words.get(j)) > 0) {
                    String temp = words.get(i);
                    words.set(i, words.get(j));
                    words.set(j, temp);
                }
            }
        }
        return words;
    }

    private class ClientTask extends AsyncTask<String, Void, Void>{

        @Override
        protected Void doInBackground(String... requests) {
            /*If join request */

            String request = requests[0];
            Log.e("request", request);
            if (request.startsWith("join")){

                /*Send a message to port 11108 AVD0*/
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt("11108"));
                    socket.setSoTimeout(2000);
                    PrintWriter pf0 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())));
                    BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    Log.e("Sent", "Request sent");
                    pf0.println(request);
                    pf0.flush();

                    String ack = br.readLine();
                    Log.e("Receive", "Ack back");

                    if (ack != null && ack.equals("ack")){
                        socket.close();
                    } else throw new IOException();

                } catch (IOException e) {
                    Log.e("AVD0", "11108 is down.");
                    successor = null;
                    predecessor = null;
                    Log.e("succ-pred", successor + "-" + predecessor);
                    e.printStackTrace();
                }

            }
            else if (request.startsWith("setnode")){
                String [] update = request.split("-");
                try{
                    Socket socket0 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(update[1]));

                    PrintWriter pf2 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket0.getOutputStream())));
                    BufferedReader br2 = new BufferedReader(new InputStreamReader(socket0.getInputStream()));
                    Log.e("Update", "Pred and Succ sent to : " + update[1]);
                    pf2.println("update-" + update[2] + "-" + update[3]);
                    pf2.flush();

                    String ack2 = br2.readLine();
                    if (ack2 != null && ack2.equals("updated")){
                        socket0.close();
                    }
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else if (request.startsWith("insert")){
                String [] insert = request.split("-");
                Socket socket1 = null;
                try {
                    socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(insert[1]));
                    socket1.setSoTimeout(2000);
                    PrintWriter pf3 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket1.getOutputStream())));
                    Log.e("Insert", "Insertion at node:" + insert[1]);

                    pf3.println("insert-" + insert[2] + "-" + insert[3]);
                    pf3.flush();

                } catch (IOException e) {
                    Log.e("Fail", "Insertion failed at:" + myPort);
                    e.printStackTrace();
                }


            } else if (request.startsWith("querysingle")){
                String [] query = request.split("-");
                Socket socket2 = null;
                try{
                    socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(query[1]));
                    socket2.setSoTimeout(2000);
                    PrintWriter pf3 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket2.getOutputStream())));
                    Log.e("Forwarrd", "Forward Query to :" + query[1]);

                    pf3.println("query-" + query[2] + "-" + query[3]);
                    pf3.flush();

                } catch (IOException e) {
                    Log.e("Fail", "Query failed at:" + myPort);
                    e.printStackTrace();

                }
            }else if (request.startsWith("foundquery")){
                String [] returnQuery = request.split("-");
                Socket socket3 = null;
                try{
                    socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(returnQuery[1]));
                    socket3.setSoTimeout(2000);
                    PrintWriter pf3 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket3.getOutputStream())));
                    Log.e("Return", "Return  Query to :" + returnQuery[1]);

                    pf3.println("return-" + returnQuery[2]);
                    pf3.flush();

                } catch (IOException e) {
                    Log.e("Fail", "Return Query failed at:" + myPort);
                    e.printStackTrace();

                }
            }else if (request.startsWith("globalquery")){
                String [] globalQ = request.split("--");
                Socket socket3 = null;
                try{
                    socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(globalQ[1]));
                    socket3.setSoTimeout(2000);
                    PrintWriter pf3 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket3.getOutputStream())));
                    Log.e("Return", "Return  Query to :" + globalQ[1]);

                    pf3.println("globalquery--" + globalQ[2] + "--" + globalQ[3] + "--" + globalQ[4]);
                    pf3.flush();

                } catch (IOException e) {
                    Log.e("Fail", "Return Query failed at:" + myPort);
                    e.printStackTrace();

                }
            }

            return null;
        }
    }
    public  void UpdatePredSucc(ArrayList<String> tempList, HashMap<String, String> tempMap){
        int n = tempList.size();
        for (int i= 0 ; i < tempList.size(); i++){
            Log.e("List", tempMap.get(tempList.get(i)));
        }

        /*Traverse the join list*/
        for (int i = 0; i < tempList.size(); i++){
            String curr = tempMap.get(tempList.get(i));
            String succ = tempMap.get(tempList.get((i + 1) % n));
            String pred = tempMap.get((tempList.get((((i - 1 % n) + n) % n))));
            Log.e("joining", pred + "-" + curr + "-" + succ);
            /*Sending pred and succ to the respective nodes*/
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "setnode-"+ curr + "-" + pred +"-" + succ);


        }

    }

    public boolean CanInsert(String k){
        try{
        if ((predecessor == null && successor == null))  {
            return true;
        }
        else if (genHash(k).compareTo(genHash(portId.get(predecessor))) > 0 && genHash(k).compareTo(nodeId) <= 0){
                    return true;
        }else if (nodeId.compareTo(genHash(portId.get(predecessor))) <=0 ){
            if (genHash(k).compareTo(genHash(portId.get(predecessor))) > 0 || genHash(k).compareTo(nodeId) <= 0){
               return  true;
            }

        }else {
            return false;
        }

        }catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return false;
    }
    public void FindNode(String k, String v){
        if (CanInsert(k)){
            Log.e("Inserting", "Inserting at " + myPort);
            InsertData(k, v);
        }else{
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert-"+ successor + "-" + k + "-" + v);

        }

    }
    public void InsertData(String k, String v){
        Context context = getContext();
        FileOutputStream outputStream;
        String filename = k;

        try {
            outputStream = context.openFileOutput(filename, context.MODE_PRIVATE);
            outputStream.write(v.getBytes());
            outputStream.close();
        } catch (Exception e) {
            Log.e(TAG, "File write failed");
        }

    }

    public void NextAvd(String successor, String data, String origin, String state){
        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "globalquery--"+successor+"--"+data+"--"+origin+"--" + state);

    }
    private class ServerTask extends AsyncTask<ServerSocket, String, Void>{

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            ServerSocket serversocket = serverSockets[0];
            Socket s = null;
            try {
                while (true) {
                    s = serversocket.accept();
                    BufferedReader br1 = new BufferedReader(new InputStreamReader(s.getInputStream()));
                    PrintWriter pp = new PrintWriter(new OutputStreamWriter(s.getOutputStream()));
                    String req = br1.readLine();
                    Log.e("Server", req);
                    Log.e("Server port", myPort);
                    if (req != null && req.startsWith("join")) {
                        String[] join = req.split("-"); // join - myport - hash of port
                        joinList.add(join[2]);
                        joinMap.put(join[2], join[1]);
                        joinList = LexoSort(joinList);
                        Log.e("Join List", "Added, Sorted List.");

                        /*Broadcast pred and succ to all nodes*/
                        UpdatePredSucc(joinList, joinMap);
                        pp.println("ack");
                        pp.flush();



                    }
                    else if (req != null && req.startsWith("update")){
                        String [] updated = req.split("-"); // update - curr - succ - pred

                        predecessor = updated[1];
                        successor = updated[2];
                        Log.e("pred", predecessor);
                        Log.e("succ", successor);
                        pp.println("Updated");
                        pp.flush();
                    }
                    else if (req != null && req.startsWith("insert")){
                        s.close();
                        String [] inserted = req.split("-");
                        FindNode(inserted[1], inserted[2]);

                    }else if (req != null && req.startsWith("query")){
                        s.close();
                        String [] Query = req.split("-");
                        ReturnOrForward(Query[1], Query[2]);

                    }else if (req != null && req.startsWith("return")){
                        s.close();
                        String [] returnQ = req.split("-");
                        queryFound = returnQ[1];
                        Log.e("Set", queryFound);

                    }else if (req != null && req.startsWith("globalquery")){
                        s.close();
                        String [] globQ = req.split("--"); // 1 - Data, 2 - Origin, 3 - GlobalFound
                        if (globQ[3].equals("true")){
                            globalfound = globQ[1];
                        } else {
                            String data = globalquery(globQ[1]);
                            if (successor.equals(globQ[2])){
                                NextAvd(successor, data, globQ[2], "true");
                            } else{
                                NextAvd(successor, data, globQ[2], "false");
                            }
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }


            return null;
        }


    }


}

