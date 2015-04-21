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
package org.apache.hadoop.fs.nfs.topology;

import java.util.Objects;

import com.google.gson.annotations.Expose;

public class GroupName {
    @Expose
    final String groupName;
    @Expose
    final String groupID;
	
	public GroupName(String groupname, String groupid) {
        this.groupName = groupname;
        this.groupID = groupid;
    }
	
	public String getGroupName() {
		return groupName;
	}
	
	public String getGroupID() {
		return groupID;
	}
	
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final GroupName other = (GroupName) obj;
        if (!Objects.equals(this.groupName, other.groupName)) {
            return false;
        }
        if (!Objects.equals(this.groupID, other.groupID)) {
            return false;
        }
        return true;
    }
 }
