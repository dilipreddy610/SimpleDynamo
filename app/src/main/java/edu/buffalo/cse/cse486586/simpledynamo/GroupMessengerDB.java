package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by dilip on 3/4/2018.
 */

import android.content.Context;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;


///ref:https://developer.android.com/reference/android/database/sqlite/SQLiteOpenHelper.html
public class GroupMessengerDB extends SQLiteOpenHelper {

    private static GroupMessengerDB sInstance;

    static final String DataBaseName="PA2.db";
    static final int Version=2;
    static final String tableName="messages_saved";

    public static synchronized GroupMessengerDB getInstance(Context context) {


        // REF:See this article for more information: http://bit.ly/6LRzfx
        if (sInstance == null) {
            sInstance = new GroupMessengerDB(context.getApplicationContext());
        }
        return sInstance;
    }

    public GroupMessengerDB(Context context){
        super(context,DataBaseName,null,Version);
    }
    @Override
    public void onCreate(SQLiteDatabase sqLiteDatabase) throws SQLException
    {
        String query="CREATE TABLE "+tableName+"(key VARCHAR PRIMARY KEY,value VARCHAR);";
        sqLiteDatabase.execSQL(query);
    }

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase,int x,int y){
        sqLiteDatabase.execSQL("DROP TABLE IF EXISTS " + tableName);
        onCreate(sqLiteDatabase);

    }
}