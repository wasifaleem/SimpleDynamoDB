package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;

public class SimpleDynamoProvider extends ContentProvider {
	private static final String TAG = SimpleDynamoProvider.class.getName();

	private Dynamo dynamo;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		return (int) dynamo.delete(selection);
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		dynamo.insert(values);
		return null;
	}

	@Override
	public boolean onCreate() {
		this.dynamo = Dynamo.get(getContext());
		return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		return dynamo.query(selection);
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		return 0;
	}

}
