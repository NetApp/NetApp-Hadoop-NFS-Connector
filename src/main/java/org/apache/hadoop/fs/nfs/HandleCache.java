/**
 * Copyright 2014 NetApp Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.nfs;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.nfs.nfs3.FileHandle;

public class HandleCache {

	private final Map<String, FileHandle> cacheMap;
	private int cacheSize;
	
	public final static Log LOG = LogFactory.getLog(HandleCache.class);
	
	public HandleCache(final int cacheSize) {

		this.cacheSize = cacheSize;
		// true = use access order instead of insertion order.
		this.cacheMap = new LinkedHashMap<String, FileHandle>(cacheSize, 0.75f, true) {
			private static final long serialVersionUID = -7556524103423461876L;

			@Override
			protected boolean removeEldestEntry(Map.Entry<String, FileHandle> eldest) {
				// When to remove the eldest entry.
				return size() > cacheSize; // Size exceeded the max allowed.
			}
		};
	}

	public synchronized void put(String key, FileHandle elem) {
		cacheMap.put(key, elem);
	}

	public synchronized FileHandle get(String key) {
		return cacheMap.get(key);
	}

	public synchronized void remove(String key) {
		cacheMap.remove(key);
	}
	
	public synchronized void removeAll(String prefix) {
	  for(Iterator<Entry<String, FileHandle>> iter = cacheMap.entrySet().iterator(); iter.hasNext(); ) {
	    Entry<String, FileHandle> entry = iter.next();
	    if(entry.getKey().startsWith(prefix)) {
	      iter.remove();
	    }
	  }
	}
	
	public synchronized void removeByValue(FileHandle handle) {
		cacheMap.values().remove(handle);
	}

	public int getCacheSize() {
		return cacheSize;
	}
}
