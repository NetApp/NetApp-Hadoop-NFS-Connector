package org.apache.hadoop.fs.nfs.topology;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.Expose;

public class SimpleUserGroupAuthentication extends UserGroupAuthentication {
    
    String nfsUserConfigFile;
    String nfsGroupConfigFile;
    
    HashMap<String, String> userToID;
    HashMap<String, String> IDToUser;
    HashMap<String, String> groupToID;
    HashMap<String, String> IDToGroup;
    
    
    @Override
    public void initialize(String userConf, String groupConf, Configuration configuration) throws IOException {
        
        nfsUserConfigFile = userConf;
        nfsGroupConfigFile = groupConf;
        
        if (nfsUserConfigFile != null) {
            File uf = new File(nfsUserConfigFile);
            if (uf.exists()) {
                createUserMap(UserMapping.loadFromFile(nfsUserConfigFile).getUserNames());
            }
        }
        
        if (nfsGroupConfigFile != null) {
            File gf = new File(nfsGroupConfigFile);
            if (gf.exists()) {
                createGroupMap(GroupMapping.loadFromFile(nfsGroupConfigFile).getGroupNames());
            }
        }

    }

    @Override
    public String getUserNameFromUserId(String userid) {
        return IDToUser.get(userid);
    }

    @Override
    public String getUserIdFromUserName(String username) {
        return userToID.get(username);
    }

    @Override
    public String getGroupNameFromGroupId(String groupid) {
        return IDToGroup.get(groupid);
    }

    @Override
    public String getGroupIdFromGroupName(String groupname) {
        return groupToID.get(groupname);
    }
    
    public void createUserMap(List<UserName> users) {
        if (users == null || users.isEmpty()) return;
        
        userToID = new HashMap<String, String> ();
        IDToUser = new HashMap<String, String> ();
        
        for(UserName user: users) {
            userToID.put(user.getUserName(), user.getUserID());
            IDToUser.put(user.getUserID(), user.getUserName());
        }
    }
    
    public void createGroupMap(List<GroupName> groups) {
        if (groups == null || groups.isEmpty()) return;
        
        groupToID = new HashMap<String, String> ();
        IDToGroup = new HashMap<String, String> ();
        
        for(GroupName group: groups) {
            groupToID.put(group.getGroupName(), group.getGroupID());
            IDToGroup.put(group.getGroupID(), group.getGroupName());
        }
    }
    
    public static class UserMapping {
        @Expose
        List<UserName> usernames;
        
        public UserMapping() {
            usernames = new LinkedList<>();
        }
        
        public UserMapping(List<UserName> usernames) {
            this.usernames = usernames;
        }
        
        public List<UserName> getUserNames() {
            return usernames;
        }
        
        public static UserMapping loadFromFile(String filename) throws IOException {
            try {
                Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
                UserMapping map = gson.fromJson(new BufferedReader(new FileReader(filename)), UserMapping.class);
                return map;
            } catch(JsonSyntaxException exception) {
                throw new IOException("Could not parse config file " + filename, exception);
            } catch(IOException exception) {
                throw new IOException("Could not open namespace config file " + filename, exception);
            }
        }
        
    }
    
    public static class GroupMapping {
        @Expose
        List<GroupName> groupnames;
        
        public List<GroupName> getGroupNames() {
            return groupnames;
        }
        
        public GroupMapping(List<GroupName> groupnames) {
            this.groupnames = groupnames;
        }
        
        public static GroupMapping loadFromFile(String filename) throws IOException {
            try {
                Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
                GroupMapping map = gson.fromJson(new BufferedReader(new FileReader(filename)), GroupMapping.class);
                return map;
            } catch(JsonSyntaxException exception) {
                throw new IOException("Could not parse config file " + filename, exception);
            } catch(IOException exception) {
                throw new IOException("Could not open namespace config file " + filename, exception);
            }
        }
        
    }

}
