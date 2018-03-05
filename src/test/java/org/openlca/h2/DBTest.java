package org.openlca.h2;

import org.junit.Test;
import org.openlca.core.database.IDatabase;
import org.openlca.core.database.NativeSql;

public class DBTest {

    @Test
    public void testDB() throws Exception {
        IDatabase db = DB.inMempory();
	    NativeSql.on(db).query("show tables", r -> {
	    	System.out.println(r.getString(1));
	    	return true;
	    });
    }
}
