package com.expedia.edw.ehcache.decorator;

import com.expedia.edw.cache.entity.Table;
import com.expedia.edw.cache.service.GrabberService;
import com.expedia.edw.cache.service.GrabberServiceImpl;
import java.io.Serializable;
import java.util.Properties;
import net.sf.ehcache.CacheException;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.constructs.CacheDecoratorFactory;
import net.sf.ehcache.constructs.EhcacheDecoratorAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Yaroslav_Mykhaylov
 */
public class DecoratorFactoryJdbc extends CacheDecoratorFactory {

    private final Logger LOG =
            LoggerFactory.getLogger(DecoratorFactoryJdbc.class);

    @Override
    public Ehcache createDecoratedEhcache(Ehcache cache, Properties properties) {
        LOG.info("Called DecoratorFactoryJdbc");
        return new DecoratorFactoryJdbc.DecoratedEhcacheJdbc(cache, properties);
    }

    @Override
    public Ehcache createDefaultDecoratedEhcache(Ehcache cache, Properties properties) {

        return new DecoratorFactoryJdbc.DecoratedEhcacheJdbc(cache, properties);
    }

    public static class DecoratedEhcacheJdbc extends EhcacheDecoratorAdapter {

        private final Logger LOG =
                LoggerFactory.getLogger(DecoratorFactoryJdbc.DecoratedEhcacheJdbc.class);
        private GrabberService grabberService = new GrabberServiceImpl();
        
        private static final int INDEX_SCHEMA_NAME = 0;
        private static final int INDEX_TABLE_NAME = 1;
        private static final int INDEX_KEY_NAME = 2;
        private static final int INDEX_VAL_NAME = 3;
        private static final int AMOUNT_MUST_ARGS = 4;
        
        public DecoratedEhcacheJdbc(Ehcache underlyingCache, Properties properties) {
            super(underlyingCache);
            LOG.info("Registred decorator for " + this.getName());
        }

        @Override
        public Element get(Object key)
                throws IllegalStateException, CacheException {
            return getData((String) key);
        }

        @Override
        public Element get(Serializable key)
                throws IllegalStateException, CacheException {
            return getData((String) key);
        }

        @Override
        public String toString() {
            return "DecoratedEhcacheJdbc[name=" + this.getName() + "]";
        }

        private Table getTable(String uri) {
            String names[] = uri.split(",");
            if (names.length != AMOUNT_MUST_ARGS) {
                throw new IllegalArgumentException("Key must have 4 words separated comma but words = " + names.length);
            }
            return new Table(names[INDEX_SCHEMA_NAME], 
                    names[INDEX_TABLE_NAME],
                    names[INDEX_KEY_NAME],
                    names[INDEX_VAL_NAME]);
        }

        private Element getData(String key) {
            LOG.info("Get cache {}", key);
            Table tbl = getTable((String) key);
            return grabberService.getData(tbl.getSchemaName() + "." + tbl.getTableName(),
                    tbl.getKeyName(), tbl.getValueName(), underlyingCache);
        }
    }
}