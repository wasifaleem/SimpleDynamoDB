package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.util.Log;

import static android.database.sqlite.SQLiteDatabase.CONFLICT_REPLACE;

public class SimpleDynamoDB extends SQLiteOpenHelper {
    private static final String TAG = SimpleDynamoDB.class.getName();
    private static SimpleDynamoDB INSTANCE = null;

    public static final String TABLE = "kv_store";
    public static final int DB_VERSION = 1;

    private SimpleDynamoDB(Context context) {
        super(context, TABLE, null, DB_VERSION);
    }

    public static SimpleDynamoDB db(Context context) {
        if (INSTANCE == null) {
            synchronized (SimpleDynamoDB.class) {
                INSTANCE = new SimpleDynamoDB(context);
            }
        }
        return INSTANCE;
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT)");
        Log.w(TAG, "Created DB");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS kv_store");
        onCreate(db);
    }

    public int drop() {
        return getWritableDatabase().delete(TABLE, null, null);
    }

    public long insert(ContentValues keyValue) {
        return getWritableDatabase().insertWithOnConflict(TABLE, null, keyValue, CONFLICT_REPLACE);
    }

    public long delete(String key) {
        return getWritableDatabase().delete(TABLE, "key = ?", new String[]{key});
    }

    public Cursor query(String key) {
        return getWritableDatabase().rawQuery("SELECT * from " + TABLE + " where key = ?", new String[]{key});
    }

    public Cursor all() {
        return getWritableDatabase().rawQuery("SELECT * from " + TABLE, null);
    }

    public long count() {
        long count = 0;
        try (Cursor cursor = getWritableDatabase().rawQuery("SELECT count(*) from " + TABLE, null)) {
            while (cursor.moveToNext()) {
                count += cursor.getLong(0);
            }
        }
        return count;
    }
}
