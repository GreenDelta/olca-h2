package org.openlca.h2;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.openlca.core.database.BaseDao;
import org.openlca.core.database.Daos;
import org.openlca.core.database.RootEntityDao;
import org.openlca.core.model.ModelType;
import org.openlca.core.model.RootEntity;
import org.openlca.core.model.descriptors.BaseDescriptor;

public class DaoTest {

    @Test
    public void testDaos() throws Exception {
        DB db = DB.empty();
        for (ModelType type : ModelType.values()) {
            Class<? extends RootEntity> c = type.getModelClass();
            if (type.getModelClass() == null)
                continue;
            check(db, c);
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends RootEntity, D extends BaseDescriptor> void check(
            DB db, Class<T> type) throws Exception {
        BaseDao<T> dao = Daos.base(db, type);
        T entity = type.newInstance();
        entity.setName("A test" + type.getSimpleName());
        dao.insert(entity);
        T clone = dao.getForId(entity.getId());
        assertEquals(entity.getName(), clone.getName());
        if (entity instanceof RootEntity) {
            RootEntityDao<T, D> rDao = (RootEntityDao<T, D>) dao;
            D descriptor = rDao.getDescriptor(entity.getId());
            assertEquals(entity.getName(), descriptor.getName());
        }
        dao.delete(entity);
    }
}