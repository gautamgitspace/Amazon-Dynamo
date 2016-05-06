package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider
{

    ArrayList<Integer> nodeSpace = new ArrayList<Integer>(5);
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    String lDump="@";
    String gDump="*";
    private int launchPort;
    static final int serverPort = 10000;
    int successorOnePort=0;
    int successorTwoPort=0;
    int predecessorOnePort=0;
    int predecessorTwoPort=0;
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs)
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values)
	{
		// TODO Auto-generated method stub
        String key = (String)values.get("key");
        String value = (String)values.get("value");
        int association=0;

        //send insert request to CT. CT calculates association and forwards to appropriate ST

        /*Sorting array list based on comparator - http://www.tutorialspoint.com/java/java_using_comparator.htm*/
        Collections.sort(nodeSpace, new Comparator<Integer>()
        {
            @Override
            public int compare(Integer here, Integer there) {
                int compareDecision = 0;
                try {
                    compareDecision = genHash(Integer.toString(here)).compareTo(genHash(Integer.toString(there)));
                }
                catch (NoSuchAlgorithmException e) {
                    Log.e(TAG, "got exception in comparator");
                }
                return compareDecision;
            }
        });

        Iterator iterator = nodeSpace.iterator();
        while(iterator.hasNext())
        {
            try
            {
                String string=(iterator.next()).toString();
                if(genHash(key).compareTo(genHash(string)) <=0)
                {
                    association=Integer.parseInt(string);
                    break;
                }
            }
            catch (NoSuchAlgorithmException e)
            {
                Log.v(TAG,"Exception in key comparison");
            }
        }

        if(association == 0)
        {
            association = nodeSpace.get(0);
        }
        Log.v(TAG, "Key: " + key + "associated with: " + association);

        if(association==launchPort)
        {
            //LOCAL INSERT
            Log.v("TAG", "insert locally");
            Log.v(TAG,"Key: " + key + "inserted locally at" + association);
            DBHandler dbHandler = new DBHandler(getContext());
            SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
            values.put("key", key);
            values.put("value",value);
            sqLiteDatabase.insert("dynamoDB", null, values);
            sqLiteDatabase.close();

            //PREPARE OBJECT FOR REPLICATED INSERT
            NodeTalk communication = new NodeTalk("replicatedInsert");
            communication.setSuccessor1(successorOnePort);
            communication.setSuccessor2(successorTwoPort);
            communication.setKey(key);
            communication.setValue(value);
            communication.setWhoAmI(launchPort);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, communication);
            Log.v(TAG, "Replicated Insert call sent to CT");
        }
        else if(association!=launchPort)
        {
            //PREPARE OBJECT FOR REDIRECTED REPLICATED INSERT
            Log.v(TAG, "redirected insert");
            NodeTalk communication = new NodeTalk("redirectedInsert");
            communication.setKey(key);
            communication.setValue(value);
            communication.setWhoAmI(launchPort);
            communication.setAssociation(association);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, communication);
            Log.v(TAG, "Redirected Insert call sent to CT");
        }


		return null;
	}

	@Override
	public boolean onCreate()
	{
		// TODO Auto-generated method stub

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        launchPort = Integer.parseInt(portStr);

        nodeSpace.add(5554);
        nodeSpace.add(5556);
        nodeSpace.add(5558);
        nodeSpace.add(5560);
        nodeSpace.add(5562);

        int myIndex=0;
        int successorOne=0;
        int successorTwo=0;

        Collections.sort(nodeSpace, new Comparator<Integer>()
        {
            @Override
            public int compare(Integer here, Integer there) {
                int compareDecision = 0;
                try {
                    compareDecision = genHash(Integer.toString(here)).compareTo(genHash(Integer.toString(there)));
                }
                catch (NoSuchAlgorithmException e) {
                    Log.e(TAG, "got exception in comparator");
                }
                return compareDecision;
            }
        });

        //now find self id, successor port 1 and successor port 2 in the array list
        for(int i=0; i<nodeSpace.size(); i++)
        {
            if(nodeSpace.get(i)==launchPort)
            {
                myIndex=i;
                break;
            }
        }
        successorOne=(myIndex+1)%5;
        successorTwo=(myIndex+2)%5;
        successorOnePort=nodeSpace.get(successorOne);
        successorTwoPort=nodeSpace.get(successorTwo);
        Log.v(TAG,"MY PORT: " + launchPort);
        Log.v(TAG,"S1 PORT: " + successorOnePort);
        Log.v(TAG,"S2 PORT: " + successorTwoPort);


        try
        {
            ServerSocket serverSocket = new ServerSocket(serverPort);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }
        catch (IOException e)
        {
            e.getMessage();
            return false;
        }
        return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder)
	{
		// TODO Auto-generated method stub
        Log.v(TAG,"inside Cursor query");
        Cursor cursor=null;
        String[] columns = {"key", "value"};
        MatrixCursor matrixCursor=new MatrixCursor(columns);

        if(selection.equals(lDump))
        {
            Log.v(TAG,"CASE LDUMP");
            DBHandler dbHandler = new DBHandler(getContext());
            SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
            cursor = sqLiteDatabase.query(true, "dynamoDB", columns, null, null, null, null, null, null);
            cursor.moveToFirst();
        }
        else if(selection.equals(gDump))
        {
            Log.v(TAG,"CASE GDUMP");
        }
        else
        {
            Log.v(TAG,"CASE SPECIFIC KEY");
            //calculate association
            String key=selection;
            int association=0;
            Iterator iterator = nodeSpace.iterator();
            while(iterator.hasNext())
            {
                try
                {
                    String string=(iterator.next()).toString();
                    if(genHash(key).compareTo(genHash(string)) <=0)
                    {
                        association=Integer.parseInt(string);
                        break;
                    }
                }
                catch (NoSuchAlgorithmException e)
                {
                    Log.v(TAG,"Exception in key comparison");
                }
            }
            if(association == 0)
            {
                association = nodeSpace.get(0);
            }
            //now find self id, successor port 1 and successor port 2 in the array list
            int myIndex=0;
            int predecessorOne=0;
            int predecessorTwo=0;

            for(int i=0; i<nodeSpace.size(); i++)
            {
                if(nodeSpace.get(i)==launchPort)
                {
                    myIndex=i;
                    break;
                }
            }
            predecessorOne=(myIndex+4)%5;
            predecessorTwo=(myIndex+3)%5;
            predecessorOnePort=nodeSpace.get(predecessorOne);
            predecessorTwoPort=nodeSpace.get(predecessorTwo);
            Log.v(TAG,"MY PORT: " + launchPort);
            Log.v(TAG,"S1 PORT: " + predecessorOnePort);
            Log.v(TAG,"S2 PORT: " + predecessorTwoPort);

            if(association==launchPort || association==predecessorOnePort || association==predecessorTwoPort)
            {
                Log.v(TAG,"local query for specific key");
                DBHandler dbHandler = new DBHandler(getContext());
                SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
                cursor = sqLiteDatabase.query(true, "dynamoDB", columns, "key=?", new String[]{selection}, null, null, null, null);
            }
            else
            {
                try
                {
                    Log.v(TAG, "Call to server of association");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), association * 2);
                    NodeTalk communication = new NodeTalk("specificKeyOther");
                    communication.setKey(selection);
                    communication.setWhoAmI(association);
                    ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                    outputStream.writeObject(communication);
                    Log.v(TAG, " specificKeyOther sent to ST on socket " + socket.toString());


                    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                    String processDaemonReply = dataInputStream.readUTF();

                    if(processDaemonReply!=null)
                    {
                        Log.v(TAG,"daemon reply processed for redirected query as : " + processDaemonReply);
                        String[] tokenContainer = processDaemonReply.split("-");

                        try {
                            matrixCursor.addRow(new String[]{tokenContainer[1], tokenContainer[2]});
                            MergeCursor mergeCursor = new MergeCursor(new Cursor[]{matrixCursor, cursor});
                            cursor = mergeCursor;
                            cursor.moveToFirst();
                            Log.v(TAG, "processDaemonReply as: " + cursor.getString(0) +"-"+ cursor.getString(1));
                        }
                        catch (NullPointerException e)
                        {
                            e.printStackTrace();
                        }
                    }
                }
                catch(IOException e)
                {
                    e.printStackTrace();
                }

            }

        }

        return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs)
    {
		// TODO Auto-generated method stub
		return 0;
	}

    private class ServerTask extends AsyncTask<ServerSocket, String, Void>
    {
        Socket socket = null;
        @Override
        protected Void doInBackground(ServerSocket... sockets)
        {
            Log.v(TAG, "%%%launch port is: " + launchPort);
            ServerSocket serverSocket = sockets[0];

            try
            {
                String daemonReply="";
                while(true)
                {
                    Log.v(TAG,"#SERVER" + launchPort + "LISTENING FOR INCOMING CONNECTIONS#");
                    socket=serverSocket.accept();
                    ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
                    NodeTalk communication = (NodeTalk)inputStream.readObject();

                    if(communication!=null)
                    {
                        if(communication.getLanguage().equals("replicatedInsert"))
                        {
                            Log.v(TAG, " Language type is replicatedInsert ");
                            daemonReply = insertHandler(communication);
                            Log.v(TAG, "Response code [1] to client sent");
                        }
                        if(communication.getLanguage().equals("redirectedInsert"))
                        {
                            Log.v(TAG, " Language type is redirectedInsert");
                            daemonReply = redirectedInsertHandler(communication);
                            Log.v(TAG, "Response code [2] to client sent");
                        }
                        if(communication.getLanguage().equals("redirectedReplicatedInsert"))
                        {
                            Log.v(TAG, " Language type is redirectedInsert");
                            daemonReply = redirectedReplicatedInsertHandler(communication);
                            Log.v(TAG, "Response code [3] to client sent");
                        }
                        if(communication.getLanguage().equals("specificKeyOther"))
                        {
                            Log.v(TAG, " Language type is specificKeyOther");
                            daemonReply = specificKeyHandler(communication);
                        }
                        try
                        {
                            Log.v(TAG, "daemonReply sent from server on socket :" + socket.toString());
                            DataOutputStream dataOutPutStream = new DataOutputStream(socket.getOutputStream());
                            dataOutPutStream.writeUTF(daemonReply);
                        }
                        catch (IOException e)
                        {
                            e.printStackTrace();
                        }
                    }

                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
            catch(ClassNotFoundException c)
            {
                c.getMessage();
            }


            return null;
        }

        String insertHandler(NodeTalk nodeTalk)
        {
            ContentValues values = new ContentValues();
            String daemonReply="";
            DBHandler dbHandler = new DBHandler(getContext());
            SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();

            values.put("key",nodeTalk.getKey());
            values.put("value", nodeTalk.getValue());
            Log.v(TAG, "Key: " + nodeTalk.getKey() + "inserted at" + launchPort);
            sqLiteDatabase.insert("dynamoDB", null, values);
            sqLiteDatabase.close();
            daemonReply="Insert successful";
            return daemonReply;
        }

        String redirectedInsertHandler(NodeTalk nodeTalk)
        {
            ContentValues values = new ContentValues();
            String daemonReply="";
            //LOCAL INSERT
            Log.v("TAG", "insert locally");
            DBHandler dbHandler = new DBHandler(getContext());
            SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
            values.put("key", nodeTalk.getKey());
            values.put("value",nodeTalk.getValue());
            Log.v(TAG, "Key: " + nodeTalk.getKey() + "inserted at" + launchPort);
            sqLiteDatabase.insert("dynamoDB", null, values);
            sqLiteDatabase.close();

            //REPLICATE ON SUCCESSORS
            try
            {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), successorOnePort * 2);
                NodeTalk communication = new NodeTalk("redirectedReplicatedInsert");
                communication.setKey(nodeTalk.getKey());
                communication.setValue(nodeTalk.getValue());
                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                outputStream.writeObject(communication);
                Log.v(TAG, "Replicate to S1");

                Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), successorTwoPort * 2);
                NodeTalk communication1 = new NodeTalk("redirectedReplicatedInsert");
                communication1.setKey(nodeTalk.getKey());
                communication1.setValue(nodeTalk.getValue());
                ObjectOutputStream outputStream1 = new ObjectOutputStream(socket1.getOutputStream());
                outputStream1.writeObject(communication1);
                Log.v(TAG, "Replicate to S2");
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }

                daemonReply = "Redirected Insert successful";
                return daemonReply;
        }
        String redirectedReplicatedInsertHandler(NodeTalk nodeTalk)
        {
            ContentValues values = new ContentValues();
            String daemonReply="";
            DBHandler dbHandler = new DBHandler(getContext());
            SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();

            values.put("key",nodeTalk.getKey());
            values.put("value", nodeTalk.getValue());
            Log.v(TAG, "Key: " + nodeTalk.getKey() + "inserted at" + launchPort);
            sqLiteDatabase.insert("dynamoDB", null, values);
            sqLiteDatabase.close();
            daemonReply="Insert successful";
            return daemonReply;
        }
        String specificKeyHandler(NodeTalk nodeTalk)
        {
            Cursor cursor = null;
            String daemonReply="";
            String key = nodeTalk.getKey();
            String[] columns = {"key", "value"};
            Log.v(TAG,"local query for specific key");
            DBHandler dbHandler = new DBHandler(getContext());
            SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
            cursor = sqLiteDatabase.query(true, "dynamoDB", columns, "key=?", new String[]{key}, null, null, null, null);

            cursor.moveToFirst();
            String value = cursor.getString(1);
            daemonReply = "singlequery-" + key + "-" +value;
            Log.d(TAG, "redirectedQueryHandler: reply from redirect query is :"+ daemonReply);
            return daemonReply;
        }


    }

    private class ClientTask extends AsyncTask<NodeTalk, Void, Cursor>
    {
        @Override
        protected Cursor doInBackground(NodeTalk... params)
        {

            Log.v(TAG,"Inside doInBackground of CT");
            try
            {
                NodeTalk obj = params[0];
                if(obj.getLanguage().equals("replicatedInsert"))
                {
                    Log.v(TAG, "Handling Simple Replicated Insert at CT");

                    //SEND TO SERVER TASK
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), obj.getSuccessor1() * 2);
                    ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                    Log.v(TAG, "about to write" + obj + "to stream");
                    outputStream.writeObject(obj);
                    Log.v(TAG, "object written with message: " + obj.getLanguage() +
                            "and source port " + obj.getWhoAmI() + "and destination port " + obj.getSuccessor1());

                    Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), obj.getSuccessor2() * 2);
                    ObjectOutputStream outputStream1 = new ObjectOutputStream(socket1.getOutputStream());
                    Log.v(TAG, "about to write" + obj + "to stream");
                    outputStream1.writeObject(obj);
                    Log.v(TAG, "object written with message: " + obj.getLanguage() +
                            "and source port " + obj.getWhoAmI() + "and destination port " + obj.getSuccessor2());

                    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                    String reply = dataInputStream.readUTF();
                    if (reply != null)
                    {
                        Log.v(TAG, "Reply received from server: " + reply);
                    }
                    else
                    {
                        Log.v(TAG, "Nothing received from server");
                    }

                }
                if(obj.getLanguage().equals("redirectedInsert"))
                {
                    Log.v(TAG, "Handling Redirected Insert at CT");
                    Log.v(TAG, "###sending to association: " + obj.getAssociation());
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), obj.getAssociation() * 2);
                    ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                    Log.v(TAG, "about to write" + obj + "to stream");
                    outputStream.writeObject(obj);
                    Log.v(TAG, "object written with message: " + obj.getLanguage() +
                            "and source port" + obj.getWhoAmI() + "and destination port" + obj.getAssociation());

                    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                    String reply = dataInputStream.readUTF();
                    if (reply != null)
                    {
                        Log.v(TAG, "Reply received from server: " + reply);
                    }
                    else
                    {
                        Log.v(TAG, "Nothing received from server");
                    }
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }

            return null;
        }
    }

    private String genHash(String input) throws NoSuchAlgorithmException
	{
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
