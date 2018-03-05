package org.openlca.h2;

import org.junit.Assert;
import org.junit.Test;
import org.openlca.core.database.FlowDao;
import org.openlca.core.database.IDatabase;
import org.openlca.core.database.NativeSql;
import org.openlca.core.model.Flow;
import org.openlca.core.model.descriptors.FlowDescriptor;

import java.util.concurrent.atomic.AtomicInteger;

public class DBTest {

	@Test
	public void testQueryTables() throws Exception {
		IDatabase db = DB.empty();
		AtomicInteger count = new AtomicInteger(0);
		NativeSql.on(db).query("show tables", r -> {
			// System.out.println(r.getString(1));
			count.incrementAndGet();
			return true;
		});
		Assert.assertTrue("There should be more than 30 tables in the database",
				count.get() > 30);
	}

	@Test
	public void testEntity() throws Exception {
		IDatabase db = DB.empty();
		Flow flow = new Flow();
		flow.setName("A test flow");
		flow.setRefId("a-test-flow");
		FlowDao dao = new FlowDao(db);
		dao.insert(flow);
		Assert.assertTrue(flow.getId() > 0L);
		Flow clone = dao.getForRefId("a-test-flow");
		Assert.assertEquals(flow, clone);
		db.close();
	}

	@Test
	public void testDescriptor() {
		IDatabase db = DB.empty();
		Flow flow = new Flow();
		flow.setName("A test flow");
		flow.setRefId("a-test-flow");
		FlowDao dao = new FlowDao(db);
		dao.insert(flow);
		FlowDescriptor d = dao.getDescriptor(flow.getId());
		Assert.assertEquals(flow.getRefId(), d.getRefId());	
	}

	@Test
	public void testDump() throws Exception {
		DB empty = DB.empty();
		Flow flow = new Flow();
		flow.setName("A test flow");
		flow.setRefId("a-test-flow");
		FlowDao dao = new FlowDao(empty);
		dao.insert(flow);
		empty.dump("dump.gz");
		empty.close();

		IDatabase dump = DB.fromDump("dump.gz");
		dao = new FlowDao(dump);
		Flow clone = dao.getForRefId("a-test-flow");
		Assert.assertEquals(flow, clone);
		dump.close();
	}
}
