package io.github.grantchen2003.cdb.tx.manager.storageengine;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class RedisStorageEngine implements StorageEngine {

    @Override
    public ItemLookupResults getItems(List<ItemLookup> itemLookups) {
        return new ItemLookupResults(0, List.of());
    }
}
