package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	static String nodeHash;
	static String portStr;
	static String myPort;
	static String prdcrNode;
	static String sucrNode;
	static String prdcrNode1;
	static String sucrNode1;
	static String keyHash;
	static String selectionHash;
	static ArrayList<String> dummy = new ArrayList<String>();
	// Ref: https://docs.oracle.com/javase/7/docs/api/java/util/TreeMap.html
	TreeMap<String, String> nodes_created = new TreeMap<String, String>();
	// Reference: https://developer.android.com/training/data-storage/sqlite.html
	SQLiteDatabase sqlDb;
	GroupMessengerDB db;
	static String KEY = "key";
	static String VALUE = "value";
	static String[] portArray = {"11124", "11112", "11108", "11116", "11120"};
	static ArrayList<String> ports = new ArrayList<String>();
	static ArrayList<String> hash_ports = new ArrayList<String>();
	static ArrayList<String> sucrArray = new ArrayList<String>();
	static ArrayList<String> prdcrArray = new ArrayList<String>();
	static String myporthash;
	static String prdcrhash;



	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		int j = 0;
        try {
            keyHash = genHash(selection);
        }
        catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Hash function not defined: ", e);
        }
        int flag = 0;
        for (int i = 0; i < hash_ports.size() - 1; i++) {
			//REF: compareTo:https://docs.oracle.com/javase/7/docs/api/java/lang/Comparable.html
            if (hash_ports.get(i).compareTo(keyHash) < 0 && hash_ports.get(i + 1).compareTo(keyHash) > 0 && flag == 0) {
                int k = i;
                flag = 1;
                if (portArray[i + 1].equalsIgnoreCase(myPort)) {
                    j=delete1(uri,selection,selectionArgs);
                    for (int p = 0; p < sucrArray.size(); p++) {
                        try {
                            Log.v("delete frwd to sucr1", sucrArray.get(p) + ":" + selection);
                            String s = "delete" + ":" + myPort + ":" + selection;
                            //REF: Socket Creation and reading : https://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
                            Socket socket221 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(sucrArray.get(p)));
                            PrintWriter out221 = new PrintWriter(socket221.getOutputStream(), true);
                            out221.println(s);
                            out221.flush();
                            BufferedReader in = new BufferedReader(new InputStreamReader(socket221.getInputStream()));
                            String st = in.readLine();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }else{
                    String s = "delete" + ":" + myPort + ":" + selection;
                    for (int p = 0; p < 3; p++) {
                        try {
                            k = (k + 1) % 5;
                            Log.v("delete forwarding to pp", portArray[k] + ":" + selection);
                            Socket socket500 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(portArray[k]));
                            PrintWriter out500 = new PrintWriter(socket500.getOutputStream(), true);
                            out500.println(s);
                            out500.flush();
                            BufferedReader in = new BufferedReader(new InputStreamReader(socket500.getInputStream()));
                            String st = in.readLine();
                        } catch (Exception e) {
                            e.printStackTrace();

                        }

                    }
                }


                }
            }
        if (keyHash.compareTo(hash_ports.get(0)) < 0 && flag == 0){
            if (portArray[0].equalsIgnoreCase(myPort)) {
                Log.v("delete in self head 0", myPort + ":" + selection);
                delete1(uri,selection,selectionArgs);
                for (int p = 0; p < sucrArray.size(); p++) {
                    try {
                        Log.v("delete frdng to sucr2", sucrArray.get(p) + ":" + selection);
                        String s = "delete" + ":" + myPort + ":" + selection;
                        Socket socket100 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(sucrArray.get(p)));
                        PrintWriter out100 = new PrintWriter(socket100.getOutputStream(), true);
                        out100.println(s);
                        out100.flush();
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket100.getInputStream()));
                        String st = in.readLine();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } else {
                Log.v("delete fwd to head frm1", myPort + ":" + selection);

                for (int p = 0; p < 3; p++) {
                    try {
                        Log.v("delete forwarding to 3", portArray[j] + ":" + selection);
                        String s = "delete" + ":" + myPort + ":" + selection;
                        Socket socket298 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(portArray[p]));
                        PrintWriter out298 = new PrintWriter(socket298.getOutputStream(), true);
                        out298.println(s);
                        out298.flush();
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket298.getInputStream()));
                        String st = in.readLine();
                    } catch (Exception e) {
                        e.printStackTrace();

                    }

                }
            }

        }
        else if(keyHash.compareTo(hash_ports.get(4)) > 0 && flag == 0){
            if (portArray[0].equalsIgnoreCase(myPort)) {
                Log.v("delete in self head 0", myPort + ":" + selection);
                delete1(uri,selection,selectionArgs);
                for (int p = 0; p < sucrArray.size(); p++) {
                    try {
                        Log.v("delete frdng to sucr2", sucrArray.get(p) + ":" + selection);
                        String s = "delete" + ":" + myPort + ":" + selection;
                        Socket socket100 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(sucrArray.get(p)));
                        PrintWriter out100 = new PrintWriter(socket100.getOutputStream(), true);
                        out100.println(s);
                        out100.flush();
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket100.getInputStream()));
                        String st = in.readLine();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } else {
                Log.v("delete fwd to head frm1", myPort + ":" + selection);

                for (int p = 0; p < 3; p++) {
                    try {
                        Log.v("delete forwarding to 3", portArray[j] + ":" + selection);
                        String s = "delete" + ":" + myPort + ":" + selection;
                        Socket socket298 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(portArray[p]));
                        PrintWriter out298 = new PrintWriter(socket298.getOutputStream(), true);
                        out298.println(s);
                        out298.flush();
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket298.getInputStream()));
                        String st = in.readLine();
                    } catch (Exception e) {
                        e.printStackTrace();

                    }

                }
            }

        }

		return j;
	}


	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		Log.v("insert first came", myPort + ":" + values.get("key"));

		try {
			keyHash = genHash((values.get("key")).toString());
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "Hash function not defined: ", e);
		}
		int flag = 0;
		for (int i = 0; i < hash_ports.size() - 1; i++) {

			if (hash_ports.get(i).compareTo(keyHash) < 0 && hash_ports.get(i + 1).compareTo(keyHash) > 0 && flag == 0) {
				int k = i;
				flag = 1;
				if (portArray[i + 1].equalsIgnoreCase(myPort)) {
					Log.v("insert came on self dd", myPort + ":" + values.get("key"));
					insert1(uri, values);
					for (int p = 0; p < sucrArray.size(); p++) {
						try {
							Log.v("insert frwd to sucr1", sucrArray.get(p) + ":" + values.get("key"));
							String s = "InsertReplica" + ":" + myPort + ":" + values.get("key") + ":" + values.get("value");
							Socket socket221 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(sucrArray.get(p)));
							PrintWriter out221 = new PrintWriter(socket221.getOutputStream(), true);
							out221.println(s);
							out221.flush();
							BufferedReader in = new BufferedReader(new InputStreamReader(socket221.getInputStream()));
							String st = in.readLine();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				} else {


						String s = "InsertReplica" + ":" + myPort + ":" + values.get("key") + ":" + values.get("value");
						for (int j = 0; j < 3; j++) {
							try {
							k = (k + 1) % 5;
							Log.v("insert forwarding to pp", portArray[k] + ":" + values.get("key"));
							Socket socket500 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(portArray[k]));
							PrintWriter out500 = new PrintWriter(socket500.getOutputStream(), true);
							out500.println(s);
							out500.flush();
							BufferedReader in = new BufferedReader(new InputStreamReader(socket500.getInputStream()));
							String st = in.readLine();
							} catch (Exception e) {
								e.printStackTrace();

						}

					}
				}
			}
		}
		if (keyHash.compareTo(hash_ports.get(0)) < 0 && flag == 0) {
			if (portArray[0].equalsIgnoreCase(myPort)) {
				Log.v("insert in self head 0", myPort + ":" + values.get("key"));
				insert1(uri, values);
				for (int p = 0; p < sucrArray.size(); p++) {
					try {
						Log.v("insert frdng to sucr2", sucrArray.get(p) + ":" + values.get("key"));
						String s = "InsertReplica" + ":" + myPort + ":" + values.get("key") + ":" + values.get("value");
						Socket socket100 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(sucrArray.get(p)));
						PrintWriter out100 = new PrintWriter(socket100.getOutputStream(), true);
						out100.println(s);
						out100.flush();
						BufferedReader in = new BufferedReader(new InputStreamReader(socket100.getInputStream()));
						String st = in.readLine();

					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} else {
				Log.v("insert fwd to head frm1", myPort + ":" + values.get("key"));

					for (int j = 0; j < 3; j++) {
						try {
						Log.v("insert forwarding to 3", portArray[j] + ":" + values.get("key"));
						String s = "InsertReplica" + ":" + myPort + ":" + values.get("key") + ":" + values.get("value");
						Socket socket298 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(portArray[j]));
						PrintWriter out298 = new PrintWriter(socket298.getOutputStream(), true);
						out298.println(s);
						out298.flush();
						BufferedReader in = new BufferedReader(new InputStreamReader(socket298.getInputStream()));
						String st = in.readLine();
						} catch (Exception e) {
							e.printStackTrace();

					}

				}
			}
            } else if (keyHash.compareTo(hash_ports.get(4)) > 0 && flag == 0) {
			if (portArray[0].equalsIgnoreCase(myPort)) {
				insert1(uri, values);
				Log.v("insert in head1 myself", myPort + ":" + values.get("key"));
				for (int p = 0; p < sucrArray.size(); p++) {
					try {
						Log.v("insert frwdi to sucr22", sucrArray.get(p) + ":" + values.get("key"));
						String s = "InsertReplica" + ":" + myPort + ":" + values.get("key") + ":" + values.get("value");
						Socket socket198 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(sucrArray.get(p)));
						PrintWriter out198 = new PrintWriter(socket198.getOutputStream(), true);
						out198.println(s);
						out198.flush();
						BufferedReader in = new BufferedReader(new InputStreamReader(socket198.getInputStream()));
						String st = in.readLine();


					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} else {
				Log.v("insert fwd to head frm2", myPort + ":" + values.get("key"));

					for (int j = 0; j < 3; j++) {
						try {
						Log.v("insert forwarding to 22", portArray[j] + ":" + values.get("key"));
						String s = "InsertReplica" + ":" + myPort + ":" + values.get("key") + ":" + values.get("value");
						Socket socket154 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(portArray[j]));
						PrintWriter out154 = new PrintWriter(socket154.getOutputStream(), true);
						out154.println(s);
						out154.flush();
						BufferedReader in = new BufferedReader(new InputStreamReader(socket154.getInputStream()));
						String st = in.readLine();
						} catch (Exception e) {
							e.printStackTrace();
					}

				}
			}

		}
		return uri;
	}


	public Uri insert1(Uri uri, ContentValues values) {
		//REF : https://developer.android.com/reference/android/database/sqlite/SQLiteDatabase

		Log.v("insert happening in rep", myPort + ":" + values.get("key")+":"+values.get("value"));
		sqlDb = db.getWritableDatabase();
		sqlDb.replace("messages_saved", null, values);

		return uri;
	}
    public int delete1(Uri uri, String selection, String[] selectionArgs){
		//REF: https://developer.android.com/reference/android/database/sqlite/SQLiteDatabase
	    int i=0;
        sqlDb = db.getWritableDatabase();
        i = sqlDb.delete("messages_saved", KEY + "=" +"'"+selection+"'", null);
        return i;
    }


	@Override
	public boolean onCreate() {
		try {
			db = new GroupMessengerDB(getContext());
			TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
			portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
			myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		} catch (NullPointerException e) {
			Log.e(TAG, "Unable to get port number: ", e);
		}
		//Hardcoding the port the ring structure:
		ports.add("5562");
		ports.add("5556");
		ports.add("5554");
		ports.add("5558");
		ports.add("5560");
		try {
			for (String s : ports) {
				nodeHash = genHash(String.valueOf(Integer.parseInt(s)));
				hash_ports.add(nodeHash);

			}
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "Hash function not defined: ", e);
		}

		Log.v("my port numb:", myPort);
		//assigning succsors and predecessors manually
		for (int i = 0; i < portArray.length; i++) {
			if (myPort.equalsIgnoreCase(portArray[i])) {
				if (i == 0) {
					sucrNode = portArray[1];
					sucrNode1 = portArray[2];
					prdcrNode = portArray[4];
					prdcrNode1 = portArray[3];
				} else if (i == 4) {
					sucrNode = portArray[0];
					sucrNode1 = portArray[1];
					prdcrNode = portArray[3];
					prdcrNode1 = portArray[2];
				} else if (i == 1) {
					sucrNode = portArray[2];
					sucrNode1 = portArray[3];
					prdcrNode = portArray[0];
					prdcrNode1 = portArray[4];
				} else if (i == 2) {
					sucrNode = portArray[3];
					sucrNode1 = portArray[4];
					prdcrNode = portArray[1];
					prdcrNode1 = portArray[0];
				} else {
					sucrNode = portArray[4];
					sucrNode1 = portArray[0];
					prdcrNode = portArray[2];
					prdcrNode1 = portArray[1];
				}
			}
		}
		sucrArray.add(sucrNode);
		sucrArray.add(sucrNode1);
		prdcrArray.add(prdcrNode);
		prdcrArray.add(prdcrNode1);
		Log.v("my succsor", sucrNode + ":" + sucrNode1);
		Log.v("my prdcr", prdcrNode + ":" + prdcrNode1);
		try{
			myporthash=genHash(String.valueOf(Integer.parseInt(myPort)/2));
			prdcrhash=genHash(String.valueOf(Integer.parseInt(prdcrNode)/2));
		}
		catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "Hash function not defined: ", e);
		}

		String msg = "failureHandling" + ":" + myPort;

		try {
			//REF: https://docs.oracle.com/javase/tutorial/rmi/client.html
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg).get();
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket: ", e);
		}catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}


        return false;
	}

	Uri uri = makeURI("content", "edu.buffalo.cse.cse486586.simpledht.provider");

	private Uri makeURI(String content, String path) {

		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.scheme(content);
		uriBuilder.authority(path);
		return uriBuilder.build();

	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        Cursor values1 = null;
        Log.v("query", myPort + ":" + selection);

        // Reference:: https://developer.android.com/reference/android/database/MatrixCursor.html
        MatrixCursor matCursor = new MatrixCursor(new String[]{KEY, VALUE});
        try {
            selectionHash = genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Hash function not defined: ", e);
        }
        if (selection.equalsIgnoreCase("@")) {
            Log.v("in query", selection);
            String[] selArgs = {selection};
            // Ref:https://developer.android.com/training/data-storage/sqlite.html

            values1 = sqlDb.query(true, "messages_saved", projection, null, null, null, null, null, null);
            values1.moveToFirst();
            if (values1.getCount() == 0) {
            }


        } else if (selection.equalsIgnoreCase("*")) {


            Cursor c = sqlDb.query(true, "messages_saved", projection, null, null, null, null, null, null);

            if (c.getCount() != 0 && c != null) {
                c.moveToFirst();
                do {
                    String[] cue = new String[2];
                    String key = c.getString(c.getColumnIndex(KEY));
                    String valu = c.getString(c.getColumnIndex(VALUE));
                    cue[0] = key;
                    cue[1] = valu;
                    matCursor.addRow(cue);
                } while (c.moveToNext());
            }

            //String s = sucrNode;
            for (String s : portArray) {
                String message = "query" + ":"+selectionHash+":" + "*";
                if (!s.equalsIgnoreCase(myPort)) {
                    try {
                        Log.v("quert all send to", s);
                        Socket query_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(s));
                        PrintWriter out2 = new PrintWriter(query_socket.getOutputStream(), true);
                        out2.println(message);
                        out2.flush();
                        BufferedReader in = new BufferedReader(new InputStreamReader(query_socket.getInputStream()));
                        String st = in.readLine();
                        String[] messages = st.split(";");
                        for (int i = 0; i < messages.length; i++) {
                            if (!messages[i].equals("")) {
                                String[] cursorEntry = messages[i].split(":");
                                matCursor.addRow(cursorEntry);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            values1 = matCursor;
        } else {
            int flag = 0;

            for (int i = 0; i < hash_ports.size() - 1; i++) {
                if (hash_ports.get(i).compareTo(selectionHash) < 0 && hash_ports.get(i + 1).compareTo(selectionHash) > 0 && flag == 0) {
                    int k = i;
                    flag = 1;
                    if (portArray[i + 1].equalsIgnoreCase(myPort)) {
                        Log.v("query on me", myPort);
                        sqlDb = db.getReadableDatabase();
                        String[] selArgs = {selection};
                        String select = "key" + "=?";
                        values1 = sqlDb.query(true, "messages_saved", projection, select, selArgs, null, null, null, null, null);
                    } else {
                        for (int j = 0; j < 2; j++) {
                            k = (k + 1) % 5;
                            try {
                                String s = "query" + ":" + selectionHash + ":" + selection;
                                Log.v("forwd query to", portArray[i + 1]);
                                Socket socket10 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(portArray[k]));
                                PrintWriter out10 = new PrintWriter(socket10.getOutputStream(), true);
                                out10.println(s);
                                out10.flush();
                                BufferedReader in = new BufferedReader(new InputStreamReader(socket10.getInputStream()));
                                String sp = in.readLine();
                                String[] messages = sp.split(";");
                                for (String str : messages) {
                                    if (!str.equals("")) {
                                        String[] cursorEntry = str.split(":");
                                        matCursor.addRow(cursorEntry);
                                    }
                                }
                                values1 = matCursor;
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }

            if (selectionHash.compareTo(hash_ports.get(0)) < 0 && flag == 0) {
                if (portArray[0].equalsIgnoreCase(myPort)) {
                    sqlDb = db.getReadableDatabase();
                    String[] selArgs = {selection};
                    String select = "key" + "=?";
                    values1 = sqlDb.query(true, "messages_saved", projection, select, selArgs, null, null, null, null, null);
                } else {
                    for (int j = 0; j < 2; j++) {
                        try {
                            String s = "query" + ":" + selectionHash + ":" + selection;
                            Socket socket9 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(portArray[j]));
                            PrintWriter out9 = new PrintWriter(socket9.getOutputStream(), true);
                            out9.println(s);
                            out9.flush();
                            BufferedReader in = new BufferedReader(new InputStreamReader(socket9.getInputStream()));
                            String sp = in.readLine();
                            String[] messages = sp.split(";");
                            for (String str : messages) {
                                if (!str.equals("")) {
                                    String[] cursorEntry = str.split(":");
                                    matCursor.addRow(cursorEntry);
                                }
                            }

                            values1 = matCursor;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }

            } else if (selectionHash.compareTo(hash_ports.get(4)) > 0 && flag == 0) {
                if (portArray[0].equalsIgnoreCase(myPort)) {
                    sqlDb = db.getReadableDatabase();
                    String[] selArgs = {selection};
                    String select = "key" + "=?";
                    values1 = sqlDb.query(true, "messages_saved", projection, select, selArgs, null, null, null, null, null);

                } else {
                    for (int j = 0; j < 2; j++) {
                        try {
                            String s = "query" + ":" + selectionHash + ":" + selection;
                            Socket socket8 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(portArray[j]));
                            PrintWriter out8 = new PrintWriter(socket8.getOutputStream(), true);
                            out8.println(s);
                            out8.flush();
                            BufferedReader in = new BufferedReader(new InputStreamReader(socket8.getInputStream()));
                            String sp = in.readLine();
                            String[] messages = sp.split(";");
                            for (String str : messages) {
                                if (!str.equals("")) {
                                    String[] cursorEntry = str.split(":");
                                    matCursor.addRow(cursorEntry);
                                }
                            }

                            values1 = matCursor;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                }
            }

        }
        return values1;
    }


	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}
    public int delete2(Uri uri, String selection, String[] selectionArgs){
        sqlDb=db.getWritableDatabase();
        int i= sqlDb.delete("messages_saved",null,null);
        return i;
    }

	public String query1(String selection){
		//REF: https://developer.android.com/reference/android/database/sqlite/SQLiteDatabase
		String cuemess="";
		sqlDb=db.getReadableDatabase();
		if(selection.equalsIgnoreCase("@")) {
			Cursor c = sqlDb.query(true, "messages_saved", null, null, null, null, null, null, null);
			if (c.getCount() != 0 && c != null) {
				Log.v("cursor count", String.valueOf(c.getCount()));
				c.moveToFirst();
				do {
					String key = c.getString(c.getColumnIndex(KEY));
					String valu = c.getString(c.getColumnIndex(VALUE));
					cuemess = cuemess + key + ":" + valu + ";";
				} while (c.moveToNext());
			} else {
				cuemess = "novalues";
			}
		}
		else if(selection.equalsIgnoreCase("*")){

		}

		return cuemess;
	}
	public Cursor query11(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder){
		//REF: https://developer.android.com/reference/android/database/sqlite/SQLiteDatabase
		Cursor values1 = null;
		Log.v("query", myPort + ":" + selection);
		sqlDb=db.getReadableDatabase();
        //MatrixCursor matCursor = new MatrixCursor(new String[]{KEY, VALUE});
		if(selection.equalsIgnoreCase("*")){
            values1 = sqlDb.query(true, "messages_saved", null, null, null, null, null, null, null);
        }
		// Reference:: https://developer.android.com/reference/android/database/MatrixCursor.html
		else{
            String[] selArgs = {selection};
            String select = "key" + "=?";
            values1 = sqlDb.query(true, "messages_saved", projection, select, selArgs, null, null, null, null, null);
        }

		return values1;
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

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			try {
				String strin = "";
				while (true) {

					Socket socket00 = serverSocket.accept();
					BufferedReader in = new BufferedReader(new InputStreamReader(socket00.getInputStream()));
					strin = in.readLine();

					if (strin != null && !strin.isEmpty()) {
						Log.v("Message in Server", strin);
						String[] str = strin.split(":");
						if (str[0].equalsIgnoreCase("InsertReplica")) {
							Log.v("IN server insert", str[2] + ":" + str[3]);
							ContentValues val = new ContentValues();
							val.put(VALUE, str[3]);
							val.put(KEY, str[2]);
							uri = insert1(uri, val);
							PrintWriter serv_out = new PrintWriter(socket00.getOutputStream(), true);
							serv_out.println(String.valueOf(uri));
							serv_out.flush();
						} else if (str[0].equalsIgnoreCase("query")) {
								Cursor c = query11(uri, null, str[2], null, null);
								String cuemess = "";
								if (c.getCount() != 0 && c != null) {
									c.moveToFirst();
									do {
										String key = c.getString(c.getColumnIndex(KEY));
										String valu = c.getString(c.getColumnIndex(VALUE));
										cuemess = cuemess + key + ":" + valu + ";";
									} while (c.moveToNext());
								}
								Log.v("Sendg back query reply",myPort);
								PrintWriter serv_out = new PrintWriter(socket00.getOutputStream(), true);
								serv_out.println(cuemess);
								serv_out.flush();

						} else if (str[0].equalsIgnoreCase("delete")) {
							int i = delete1(uri, str[2], null);
							PrintWriter serv_out = new PrintWriter(socket00.getOutputStream(), true);
							serv_out.println(i);
							serv_out.flush();
						}
						else if(str[0].equalsIgnoreCase("queryrecover")){
							String q=query1("@");
							PrintWriter serv_out = new PrintWriter(socket00.getOutputStream(), true);
							serv_out.println(q);
							serv_out.flush();
						}
					}
				}
			} catch (IOException e) {
				Log.e(TAG, "Socket IO exception");
			}

			return null;
		}

	}
//23
	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			try {
				String send = msgs[0];
				String[] msg_type = send.split(":");
				if (msg_type[0].equalsIgnoreCase("failureHandling")) {


                    int p=delete2(uri,"@",null);
					String s = "queryrecover" + ":" + "@";
					String hash="";
					int dummy=100;
					for (int i = 0; i < prdcrArray.size(); i++){
						for(int z=0;z<5;z++){
							if(prdcrArray.get(i).equalsIgnoreCase(portArray[z])){
								dummy=z;
							}
						}
						try {
							Log.v("after recover to prdr", prdcrArray.get(i));
							Socket query_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(prdcrArray.get(i)));
							PrintWriter out2 = new PrintWriter(query_socket.getOutputStream(), true);
							out2.println(s);
							out2.flush();
							BufferedReader in = new BufferedReader(new InputStreamReader(query_socket.getInputStream()));
							String st = in.readLine();
							if (!st.equalsIgnoreCase("novalues")) {
								String[] messages = st.split(";");
								Log.v("msg received from prdcr", ":" + messages.length);
								try {

									for (int j = 0; j < messages.length; j++) {
										if (!messages[j].equals("")) {
											String[] cursorEntry = messages[j].split(":");
											ContentValues val = new ContentValues();
											val.put(VALUE, cursorEntry[1]);
											val.put(KEY, cursorEntry[0]);
											try{
												hash=genHash(cursorEntry[0]);
											}catch (NoSuchAlgorithmException e) {
												Log.e(TAG, "Hash function not defined: ", e);
											}
											String hashkey = "";
											if(prdcrArray.get(i).equalsIgnoreCase(portArray[0])){
												if(hash_ports.get(0).compareTo(hash)>0){
													uri = insert1(uri, val);
												}
												else if(hash_ports.get(4).compareTo(hash)<0){
													uri = insert1(uri, val);
												}

											}
											else{
													if(hash.compareTo(hash_ports.get(dummy-1))>0 && hash.compareTo(hash_ports.get(dummy))<0) {
														insert1(uri, val);
													}

											}

										}
									}
								} catch (Exception e) {
									Log.e(TAG, "[Sync]: Ex: ", e);
								}
							}

						} catch (Exception e) {
							e.printStackTrace();
						}
				}

					//String s="queryrecover" + ":" + "@";
					try{
						Log.v("after recover to sucr",sucrNode);
						Socket query_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(sucrNode));
						PrintWriter out2 = new PrintWriter(query_socket.getOutputStream(), true);
						out2.println(s);
						out2.flush();
						BufferedReader in = new BufferedReader(new InputStreamReader(query_socket.getInputStream()));
						String st = in.readLine();
						if(!st.equalsIgnoreCase("novalues")){
							String[] messages = st.split(";");
							Log.v("msgs received from sucr",":"+messages.length);
                            try {
                                for (int j = 0; j < messages.length; j++) {
                                    if (!messages[j].equals("")) {
                                        String[] cursorEntry = messages[j].split(":");
                                        ContentValues val = new ContentValues();
                                        val.put(VALUE, cursorEntry[1]);
                                        val.put(KEY, cursorEntry[0]);
										try{
											hash=genHash(cursorEntry[0]);
										}catch (NoSuchAlgorithmException e) {
											Log.e(TAG, "Hash function not defined: ", e);
										}
                                        String hashkey = "";
                                        if(myPort.equalsIgnoreCase(portArray[0])){
                                        	if(hash.compareTo(hash_ports.get(0))<0){
												uri = insert1(uri, val);
											}
											else if(hash.compareTo(hash_ports.get(4))>0)
											{
												uri = insert1(uri, val);
											}
										}
										else{
                                        	if(myporthash.compareTo(hash)>0 && prdcrhash.compareTo(hash)<0){
												uri = insert1(uri, val);
											}
										}

                                    }
                                }
                            }catch (Exception e){
                                Log.e(TAG, "[Sync]: Ex: ", e);
                            }
						}

					} catch (Exception e) {
						e.printStackTrace();
					}

				}


			} catch (Exception e) {
				Log.e(TAG, "Unknown exception");
				e.printStackTrace();
			}
			return null;
		}
	}
}