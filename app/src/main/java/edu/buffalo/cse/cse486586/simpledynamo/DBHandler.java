package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by Gautam on 4/14/16.
 */
public class DBHandler extends SQLiteOpenHelper
{
    private static  String dbName="mainTuple";
    private static int version=1;

    private static final String schema = "CREATE TABLE dynamoDB (key DATA, value DATA, association DATA)";

    public DBHandler(Context context)
    {
        super(context, dbName, null, version);
    }

    @Override
    public void onCreate(SQLiteDatabase db)
    {
        db.execSQL(schema);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int obsolete, int latest)
    {
        //logic understood from - http://stackoverflow.com/questions/3675032/drop-existing-table-in-sqlite-when-if-exists-operator-is-not-supported
        db.execSQL("DROP TABLE IF EXISTS dhtRecords");
        onCreate(db);
    }

}
