package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.os.SystemClock;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	String TAG = SimpleDynamoProvider.class.getSimpleName();
	String currNode = "";
	String currPort = "";
	String predNode = "";
	String succNode = "";
	HashMap<String,String> insertFlagMap = new HashMap<String, String>();


	boolean failureRec;
	//HashMap<String,Integer> versionMap = new HashMap<String, Integer>();

	HashMap<String,Long> failureVersioning = new HashMap<String, Long>();
	HashMap<String,Node> nodeTable;
	HashMap<String,String> missedKeyVals = new HashMap<String,String>();
	HashMap<String,Integer> acceptBQ = new HashMap<String, Integer>();


	BlockingQueue<String> insertBq = new ArrayBlockingQueue<String>(5);
    BlockingQueue<String> starBq = new ArrayBlockingQueue<String>(5);
	BlockingQueue<String> failureBq = new ArrayBlockingQueue<String>(5);



	ContentResolver contentProvider;
	Uri gUri;




	/**
	 * ATTENTION: This was auto-generated to implement the App Indexing API.
	 * See https://g.co/AppIndexing/AndroidStudio for more information.
	 */


	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}


	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub



		Log.d(TAG,"CurrentTimeIs: "+ System.currentTimeMillis());

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		currNode = portStr;
		currPort = myPort;

		nodeTable = new HashMap<String, Node>();

		nodeTable.put("5554",new Node("5554","5558","5556"));
		nodeTable.put("5556",new Node("5556","5554","5562"));
		nodeTable.put("5558",new Node("5558","5560","5554"));
		nodeTable.put("5560",new Node("5560","5562","5558"));
		nodeTable.put("5562",new Node("5562","5556","5560"));


		predNode = nodeTable.get(currNode).getPred();
		succNode = nodeTable.get(currNode).getSucc();

		contentProvider = getContext().getContentResolver();
		gUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");


		failureRec = false;
		try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
			ServerSocket serverSocket = new ServerSocket(10000);


			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

			//new DeliverTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort);
		} catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
			Log.e(TAG, "Can't create a ServerSocket");
			//return;
		}


		/*try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}*/

		Log.d(TAG,"CrashedAndRecovered");


		//Cursor cursor = contentProvider.query(gUri,null,"*",null,null);



		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"failureRecover");

		//Cursor cursor = contentProvider.query(gUri,null,"*",null,null);
		//sendPkt("Testing","11108","TestAck","TestLog");

		//Log.d(TAG,"CurrentTimeIs: "+ System.currentTimeMillis());

		return false;
	}



	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			BufferedReader reader;
			//serverSocket.toString();

			String data = "";

			try {

				while (true) {

					Socket clientSocket = serverSocket.accept();



					BufferedReader in = new BufferedReader(
							new InputStreamReader(clientSocket.getInputStream()));

					data = in.readLine();

					DataOutputStream outStream = new DataOutputStream(clientSocket.getOutputStream());

					if(data.startsWith("insert")){
						Log.d(TAG,"ReceivedInsertMessage: "+data);

						String[] info = data.split(":");
						String key = info[1].trim();


						ContentValues cv = new ContentValues();

						String value = info[2].trim();

						insertFlagMap.put(key+":"+value,"y");
						cv.put("key", key);
						cv.put("value", value);
						contentProvider.insert(gUri, cv);

						outStream.writeBytes("InsertAck\n");



					}
					if(data.startsWith("query")){
						Log.d(TAG,"RecievedQueryMessage: "+data);

						String[] info = data.split(":");

						String key = info[1].trim();



						String [] selArgs  = new String[]{"secEntry"};
						Cursor cursor = contentProvider.query(gUri,null,key,selArgs,null);

						String out = "";
						if (cursor.moveToFirst()){
							do{
								String k1 = cursor.getString(cursor.getColumnIndex("key"));
								String v1 = cursor.getString(cursor.getColumnIndex("value"));
								out+=k1+"-"+v1+":";

								// do what ever you want here
							}while(cursor.moveToNext());
						}
						cursor.close();


						outStream.writeBytes("queryAck::"+out+"\n");

					}
					if(data.startsWith("star")){

						Cursor cursor = contentProvider.query(gUri,null,"@",null,null);

						String out = "";
						if (cursor.moveToFirst()){
							do{
								String k1 = cursor.getString(cursor.getColumnIndex("key"));
								String v1 = cursor.getString(cursor.getColumnIndex("value"));
								out+=k1+"-"+v1+":";

								// do what ever you want here
							}while(cursor.moveToNext());
						}
						cursor.close();

						if(out.isEmpty()){
							out="*";
						}
						//Log.d(TAG,"RetrievedData::"+sender+":::"+out);

						outStream.writeBytes("starAck::"+out+ "\n");
					}
					if(data.startsWith("failure")){




						Log.d(TAG,"ServerRec: FailureRecRequest: "+currNode);

						String[] selArgs = new String[]{"FailureRec"};
						Cursor cursor = contentProvider.query(gUri,null,"@",selArgs,null);

						//Log.d(TAG,"Maybe");
						String out = "";


						if (cursor.moveToFirst()){
							do{
								String k1 = cursor.getString(cursor.getColumnIndex("key"));
								String v1 = cursor.getString(cursor.getColumnIndex("value"));
								out+=k1+"-"+v1+":";

								// do what ever you want here
							}while(cursor.moveToNext());
						}
						cursor.close();

						if(out.isEmpty()){
							out="*";
						}
						//Log.d(TAG,"RetrievedData::"+sender+":::"+out);

						//outStream.writeBytes("starAck::"+out+ "\n");

						outStream.writeBytes("failureAck::"+out+"\n");
					}

					if(data.startsWith("delete")){

						String key = data.split(":")[1].trim();

						String[] selArgs = new String[]{"deleteRep"};
						contentProvider.delete(gUri,key,selArgs);

						outStream.writeBytes("DeleteAck\n");

					}

					//clientSocket.close();

				}

			} catch (Exception e) {


			}

		  return null;
		}
	}

	public boolean keyBelongsToNode(String key,String node){

		String keyHash = null;
		try {
			keyHash = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		Node n = nodeTable.get(node);

		return (keyHash.compareTo(n.getPredId())>0 && keyHash.compareTo(n.getNodeId())<0)
				|| (n.getPredId().compareTo(n.getNodeId()) >0 && keyHash.compareTo(n.getPredId())>0 )
				|| (n.getPredId().compareTo(n.getNodeId()) >0 && keyHash.compareTo(n.getNodeId())<0 );
	}

	public ArrayList<String> getPrefList(String key){

		String coordNode="";
		ArrayList<String> repList = new ArrayList<String>();
		String keyHash="";
		try {
			keyHash = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		for(String k:nodeTable.keySet()){
			//Log.d(TAG,"Checking Node: "+k);
			Node n = nodeTable.get(k);

			//Log.d(TAG,"Comparing "+keyHash+": "+n.getPredId()+" :"+n.getNodeId());
			if( (keyHash.compareTo(n.getPredId())>0 && keyHash.compareTo(n.getNodeId())<0)
					|| (n.getPredId().compareTo(n.getNodeId()) >0 && keyHash.compareTo(n.getPredId())>0 )
					|| (n.getPredId().compareTo(n.getNodeId()) >0 && keyHash.compareTo(n.getNodeId())<0 )) {


				//Log.d(TAG,"Wtf This shouldnt happen");
				coordNode = k;
				break;
			}

		}

		repList.add(coordNode);
		String next = nodeTable.get(coordNode).getSucc();
		repList.add(next);
		String last = nodeTable.get(next).getSucc();
		repList.add(last);

		//Log.d(TAG,"FinalRepList:"+repList);
		return repList;
	}

	private class ClientTask extends AsyncTask<Object, Void, Void> {




		@Override
		protected Void doInBackground(Object... msgs) {
			String msgToSend = msgs[0] + "\n";



			Log.d(TAG,"Received Message: "+msgToSend);
			//sendPkt("Testing","11108","TestAck","TestLog");
			if(msgToSend.startsWith("failure")){


				contentProvider.delete(gUri,"@",null);

				Log.d(TAG,"Testing out failure");

				String allVals="";

				String pred = nodeTable.get(currNode).getPred();
				String succ = nodeTable.get(currNode).getSucc();

				ArrayList<String> failRecNodes = new ArrayList<String>();

				failRecNodes.add(pred);
				failRecNodes.add(succ);

				for(String node:nodeTable.keySet()){
					try {

						if(node.equals(currNode))
							continue;
						Log.d(TAG,"EnteredFailureRec: "+msgToSend);


						//String key = info[1].trim();

						//Log.d(TAG,"QueryingKey: "+key);
						String remotePort = Integer.toString(Integer.parseInt(node) * 2);
						//Log.d(TAG,"Sending Query To"+key+": "+remotePort);
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(remotePort));

						//msgToSend = msgToSend.trim() + ":"+currNode+"\n";

						DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());
						outStream.writeBytes(msgToSend);

						BufferedReader in = new BufferedReader(
								new InputStreamReader(socket.getInputStream()));

						String ack = in.readLine();
						if (ack.startsWith("failureAck")) {

							String[] inf = ack.split("::");

							if(!inf[1].trim().equals("*"))
								allVals += inf[1].trim();

							Log.d(TAG,"FailureAck Successfull");
							socket.close();

						}
					}catch (Exception e){
						Log.d(TAG,"FailureRecException: "+e);
					}
				}


				ContentValues[] missedResults ;
				List<ContentValues> missedResList = new ArrayList<ContentValues>();

				if(!allVals.isEmpty()&& allVals.length()>1){

					String[] KeyVals = allVals.split(":");

					for(String kvP: KeyVals){

						//Log.d(TAG,"UpdatingAfterFailure: "+kvP);
						String[] kvPair = kvP.split("-");
						if(kvPair.length<2)
							continue;
						String key = kvPair[0];

						String[] valueInfo = kvPair[1].split("##");
						long curVersion = 0;

						Log.d(TAG,"PossibleRecKeyVals: "+kvP);

						String value = valueInfo[0];
						long  version = Long.parseLong(valueInfo[1]);

						if(getPrefList(key).contains(currNode)) {
							if (failureVersioning.containsKey(key)) {
								curVersion = failureVersioning.get(key);

							}

							if (version-curVersion>0) {

								Log.d(TAG,"UpdatingAfterFailure: "+kvP);
								failureVersioning.put(key,version);
								//versionMap.put(key,version);
								missedKeyVals.put(key,kvPair[1]);

								//ContentValues cv = new ContentValues();


								//String value = info[2].trim();

								//insertFlagMap.put(key+":"+kvPair[1],"y");
								//cv.put("key", key);
								//cv.put("value", kvPair[1]);

								//missedResList.add(cv);
								//contentProvider.insert(gUri, cv);

							}

						}
					}

					for(String k:missedKeyVals.keySet()){

						ContentValues cv = new ContentValues();



						//String value = info[2].trim();

						//insertFlagMap.put(key+":"+kvPair[1],"y");
						cv.put("key", k);
						cv.put("value", missedKeyVals.get(k));

						Log.d(TAG,"AddingMissedVals: "+k+":"+missedKeyVals.get(k));


						contentProvider.insert(gUri,cv);
						missedResList.add(cv);

					}

					missedResults = new ContentValues[missedResList.size()];
					missedResList.toArray(missedResults);



					//if(missedResList.size()>0)
					//	contentProvider.bulkInsert(gUri,missedResults);

				}

				failureRec = true;

			}

			if (msgToSend.startsWith("insert")) {


					int insertCount =0;
					Log.d(TAG,"EnteredInsert"+msgToSend);
					String[] info = msgToSend.split(":");

					String key = info[1].trim();

					Log.d(TAG,"InsertionKey: "+key);

					ArrayList<String> repNodes = getPrefList(key);
					if(repNodes.contains(currNode)) {
						repNodes.remove(currNode);
						insertCount+=1;
					}

					Log.d(TAG,"RepNodes: "+repNodes.toString());
					for (String node : repNodes) {
						try {

							String remotePort = Integer.toString(Integer.parseInt(node) * 2);
							Log.d(TAG, "Sending Insert " + key + ": " + remotePort);
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(remotePort));


							DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());

							outStream.writeBytes(msgToSend);

							BufferedReader in = new BufferedReader(
									new InputStreamReader(socket.getInputStream()));

							String ack = in.readLine();
							if (ack.startsWith("InsertAck")) {
								socket.close();

								insertCount+=1;
								if(insertCount==2){
									insertBq.put("Done");
								}

							}


						} catch (Exception e) {
							Log.d(TAG, "InsertionException: " + e);
						}

					}



			}
			if(msgToSend.startsWith("query")){


                    BlockingQueue<String> queryBq = (BlockingQueue<String>)  msgs[1];
				 	int queryCount=0;
					Log.d(TAG,"EnteredQuery"+msgToSend);
					String[] info = msgToSend.split(":");

					String key = info[1].trim();
					ArrayList<String> repNodes = getPrefList(key);

					if(repNodes.contains(currNode)) {
						queryCount+=1;
						repNodes.remove(currNode);
					}

					String queryKeyVals = "";
					for(String node:repNodes) {

						try {

						Log.d(TAG, "QueryingKey: " + key);
						String remotePort = Integer.toString(Integer.parseInt(node) * 2);
						Log.d(TAG, "Sending Query To" + key + ": " + remotePort);
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(remotePort));

						//msgToSend = msgToSend.trim() + ":" + currNode + "\n";

						DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());


						outStream.writeBytes(msgToSend);

						BufferedReader in = new BufferedReader(
								new InputStreamReader(socket.getInputStream()));

						String ack = in.readLine();
						if (ack.startsWith("queryAck")) {

							queryCount+=1;

							String[] queryResult = ack.split("::");
							if(queryResult[1].contains("##"))
								queryKeyVals += queryResult[1];

							socket.close();

							if(queryCount>=2)
								break;
						}

					}catch (Exception e){
						Log.d(TAG,"QueryingException: "+e);
					}


					}

				Log.d(TAG,"ReceivedQueryResults: "+queryKeyVals);
				//acceptBQ.put(key,1);
				try {
                    queryBq.put(queryKeyVals);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}


			}


			if(msgToSend.startsWith("starQuery")){

				Log.d(TAG,"EnteredClient: StarQuery");
				String allVals="";
				for(String node:nodeTable.keySet()){
					if(node.equals(currNode))
						continue;

					try {
						Log.d(TAG,"EnteredStarQuery: "+msgToSend);


						//String key = info[1].trim();

						//Log.d(TAG,"QueryingKey: "+key);
						String remotePort = Integer.toString(Integer.parseInt(node) * 2);
						//Log.d(TAG,"Sending Query To"+key+": "+remotePort);
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(remotePort));

						//msgToSend = msgToSend.trim() + ":"+currNode+"\n";

						DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());
						outStream.writeBytes(msgToSend);

						BufferedReader in = new BufferedReader(
								new InputStreamReader(socket.getInputStream()));

						String ack = in.readLine();
						if (ack.startsWith("starAck")) {

							String[] inf = ack.split("::");

							if(!inf[1].trim().equals("*"))
								allVals += inf[1].trim();

							socket.close();

						}


					}catch (Exception e){
						Log.d(TAG,"StarQueryingException: "+e);
					}
				}

				try {
					starBq.put(allVals);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}

			if(msgToSend.startsWith("delete")){

					Log.d(TAG,"EnteredDelete: "+msgToSend);
					String[] info = msgToSend.split(":");

					String key = info[1].trim();

					//Log.d(TAG,"InsertionKey: "+key);

					ArrayList<String> repNodes = getPrefList(key);

					if(repNodes.contains(currNode))
						repNodes.remove(currNode);


					//Log.d(TAG,"RepNodes: "+repNodes.toString());
					for (String node : repNodes) {
						try {

							String remotePort = Integer.toString(Integer.parseInt(node) * 2);
							//Log.d(TAG, "Sending Insert " + key + ": " + remotePort);
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(remotePort));


							DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());

							outStream.writeBytes(msgToSend);

							BufferedReader in = new BufferedReader(
									new InputStreamReader(socket.getInputStream()));

							String ack = in.readLine();
							if (ack.startsWith("DeleteAck")) {
								socket.close();
							}


						} catch (Exception e) {
							Log.d(TAG, "DeletionException: " + e);
						}

					}

			}
			return null;
		}
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		Log.d(TAG,"Entered Delete: "+selection);






		if(selection.equals("*") || selection.equals("@")) {
			File[] files = getContext().getFilesDir().listFiles();
			for (File file : files) {

				file.delete();

			}

			/*if(selection.equals("*") && !predNode.isEmpty()){
				String deleteMsg = "Delete:"+succNode;
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,deleteMsg , cNode);
			}*/
		}
		else {

			File dir = getContext().getFilesDir();
			File file = new File(dir,selection);
			if(file.exists()) {
				file.delete();
				//Log.d(TAG,"Deleting "+selection+" at: "+cNode);
			}

			if(selectionArgs==null || selectionArgs.length==0)
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "deleteKey:" + selection);

				Log.d(TAG,"NeedToRouteDelete");
				//Log.d(TAG,"Couldnt delete: "+selection+ "passing to "+ succNode);
			}


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

		FileOutputStream outputStream;
		boolean first = false;

		Log.d(TAG, "Inserting: " + key + "-" + value);
		ArrayList<String> repList = getPrefList(key);

		if(!value.contains("#")){
			value= value+ "##" + System.currentTimeMillis();
			first = true;
		}


		if (!first || repList.contains(currNode)) {


			    try {

					//Log.d(TAG, "Inserting: " + key + "-" + value);


					//outputStream = openFileOutput(key, Context.MODE_PRIVATE);

					File dir = getContext().getFilesDir();
					File file = new File(dir,key);

					if(file.exists()){

						String existingValue = fetchQueryVal(key);
						long oldVersion = Long.parseLong(existingValue.split("##")[1]);

						long currVersion = Long.parseLong(value.split("##")[1]);
						if(oldVersion-currVersion>0)
							value = existingValue;

					}

					Log.d(TAG, "ActuallyInserted: " + key + "-" + value);
					outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
					outputStream.write(value.getBytes());
					outputStream.close();

					} catch (Exception e) {
						Log.e("GroupMessenger", "File write failed");
					}

		}
		if (first) {
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "inserting:" + key + ":" + value);



			try {
				String res = insertBq.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}


		}


		Log.d(TAG, "InsertCallReturned: " + key + "-" + value);
		return uri;
	}



	public String fetchQueryVal(String selection){
		String value = "";
		try {
			StringBuilder builder = new StringBuilder();
			FileInputStream inputStream = getContext().openFileInput(selection);
			int ch;
			while ((ch = inputStream.read()) != -1) {
				builder.append((char) ch);
			}
			value = builder.toString();
			inputStream.close();

		} catch (Exception e) {
			Log.e(TAG, "ExceptionFile:" + e);
		}


		return value;

	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub




		if(selectionArgs==null){

			while(!failureRec){
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}



		String columnNames[] = {"key","value"};
		MatrixCursor cursor = new MatrixCursor(columnNames,1);

		if(selection.equals("*") || selection.equals("@")) {




			Log.d(TAG,"EnteredStarCP: "+selection);
			File[] files = getContext().getFilesDir().listFiles();
			for (File file : files) {

				String filename = file.toString();
				String pattern = Pattern.quote(System.getProperty("file.separator"));
				String[] splittedFileName = filename.split(pattern);

				String s = file.toString();

				if(selectionArgs==null) {
					//Thread.sleep();
					cursor = fetchValue(cursor, splittedFileName[splittedFileName.length - 1]);
				}
				else{
					cursor = fetchValueNoSplit(cursor, splittedFileName[splittedFileName.length - 1]);
				}
					//Log.d(TAG, "Logging files: " + splittedFileName[splittedFileName.length - 1]);
			}

			if (selection.equals("*")) {
				Log.d(TAG,"AboutToCallClient: "+selection);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "starQuery");


				try {
					String starData = "";
					starData = starBq.take();

					if (starData!=null || !starData.isEmpty()) {
						String[] kvPairs = starData.split(":");

						for (String kv : kvPairs) {
							Log.d(TAG, "KVPair:" + kv);
							String[] kvOut = kv.split("-");

							//String value = kvOut[1].split("##")[1];
							cursor.addRow(new Object[]{kvOut[0],  kvOut[1]});

						}
					}


				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
		}
		else if(selectionArgs==null || selectionArgs.length==0){

                BlockingQueue<String> bq = new ArrayBlockingQueue<String>(5);

                ArrayList<String> prefList = getPrefList(selection);
				String keyVals = "";

				Log.d(TAG,"OriginalQueryAt: "+selection+": "+currNode+"--"+prefList);
			    if(prefList.contains(currNode)){

					String val = fetchQueryVal(selection);
					keyVals+= selection+"-"+val+":";
				}

				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"querying:"+selection,bq);

                /*
				while(!acceptBQ.containsKey(selection)){

					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}*/
				try {
					keyVals+= bq.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}


				//acceptBQ.remove(selection);
				Log.d(TAG,"RetrievedKeyVals: "+keyVals);
				String key = "";
				String value ="";
				long latestVersion = 0;
				for(String kvPair:keyVals.split(":")){

					if(kvPair.length()<=1)
						continue;
					String[] kv = kvPair.split("-");
					if(kv.length<2)
						continue;
					String[] versionInfo = kv[1].split("##");

					long versionStamp = Long.parseLong(versionInfo[1]);
					if(versionStamp-latestVersion>0){


						latestVersion = versionStamp;
						key = kv[0];
						value = versionInfo[0];
					}
				}

				Log.d(TAG,"ReturningKeyVals: "+key+"-"+value);
				cursor.addRow(new Object[]{key,value});
		}
		else{

			Log.d(TAG,"ReturningQueryResults: "+selection+" :"+currNode);
			String val = fetchQueryVal(selection);

			cursor.addRow(new Object[]{selection,val});
		}

		return cursor;
	}

	public MatrixCursor fetchValue(MatrixCursor cursor,String selection){


		String value="";
		try {
			StringBuilder builder = new StringBuilder();
			FileInputStream inputStream = getContext().openFileInput(selection);
			int ch;
			while ((ch = inputStream.read()) != -1) {
				builder.append((char) ch);
			}
			value = builder.toString();
			inputStream.close();
		} catch (FileNotFoundException e) {
			Log.e("GroupMessenger", "Unable to read file");
		} catch (IOException e) {
			e.printStackTrace();
		}

		String[] valueSplit = value.split("##");
		String actualVal = valueSplit[0];
		cursor.addRow(new Object[]{selection,actualVal });
		return cursor;
	}


	public MatrixCursor fetchValueNoSplit(MatrixCursor cursor,String selection){


		String value="";
		try {
			StringBuilder builder = new StringBuilder();
			FileInputStream inputStream = getContext().openFileInput(selection);
			int ch;
			while ((ch = inputStream.read()) != -1) {
				builder.append((char) ch);
			}
			value = builder.toString();
			inputStream.close();
		} catch (FileNotFoundException e) {
			Log.e("GroupMessenger", "Unable to read file");
		} catch (IOException e) {
			e.printStackTrace();
		}

		Log.d(TAG,"Logging files @ query: "+selection+"-"+value);
		//String[] valueSplit = value.split("##");
		//String actualVal = valueSplit[0];
		cursor.addRow(new Object[]{selection,value });
		return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
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

	private class Node {
		//private Node succ = null;
		//private Node pred = null;
		private String currId;
		//private String node_id;
        private String succId;
		private String predId;
		private String succ;
		private String pred;

		public Node(String curr, String succ, String pred) {

			try{
			this.currId = genHash(curr);
			//this.node_id = node_id;
			this.succ = succ;
			this.pred = pred;

		    this.succId = genHash(succ);
			this.predId = genHash(pred);

			}catch (Exception e){

			}
		}

		public String getPred() {
			return this.pred;
		}

		public String getSucc() {
			return this.succ;
		}

		public String getNodeId() { return this.currId;}

		public String getSuccId() {
			return this.succId;
		}

		public String getPredId() {
			return this.predId;
		}

	}
}
