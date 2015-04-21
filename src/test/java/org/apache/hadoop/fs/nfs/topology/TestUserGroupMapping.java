package org.apache.hadoop.fs.nfs.topology;

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.LinkedList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class TestUserGroupMapping {
    
    static List<UserName> users;
    static UserName user1, user2, user3, user4;
    static List<GroupName> groups;
    static GroupName group1, group2, group3, group4;
    
    
    @BeforeClass
    public static void setup() {
        users = new LinkedList<>();
        user1 = new UserName("user1", "101");
        user2 = new UserName("user2", "102");
   
        groups = new LinkedList<>();
        group1 = new GroupName("group1", "201");
        group2 = new GroupName("group2", "202");
        
        users.add(user1);
        users.add(user2);
        
        groups.add(group1);
        groups.add(group2);

    }
    @Test
    public void testGenerateUserGroupMapping() throws Exception {
        
          
        SimpleUserGroupAuthentication ug = new SimpleUserGroupAuthentication();
        ug.createUserMap(users);
        ug.createGroupMap(groups);
        
        assertEquals(ug.getUserIdFromUserName("user1"), "101");
        assertEquals(ug.getUserIdFromUserName("user2"), "102");
        assertEquals(ug.getUserNameFromUserId("101"), "user1");
        assertEquals(ug.getUserNameFromUserId("102"), "user2");
           
        assertEquals(ug.getGroupIdFromGroupName("group1"), "201");
        assertEquals(ug.getGroupIdFromGroupName("group2"), "202");
        assertEquals(ug.getGroupNameFromGroupId("201"), "group1");
        assertEquals(ug.getGroupNameFromGroupId("202"), "group2");

    }
    
    @Test
    public void testGenerateFromFile() throws Exception {
        
        SimpleUserGroupAuthentication.UserMapping UserMapping = new SimpleUserGroupAuthentication.UserMapping(users);
        BufferedWriter writer = new BufferedWriter(new FileWriter("/tmp/user-mapping.json"));
        Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create();
        writer.append(gson.toJson(UserMapping));
        writer.flush();
        writer.close();
        
        SimpleUserGroupAuthentication.GroupMapping GroupMapping = new SimpleUserGroupAuthentication.GroupMapping(groups);
        writer = new BufferedWriter(new FileWriter("/tmp/group-mapping.json"));
        writer.append(gson.toJson(GroupMapping));
        writer.flush();
        writer.close();
        
        NamespaceOptions options = new NamespaceOptions();
        options.setNfsUserConfigFile("/tmp/user-mapping.json");
        options.setNfsGroupConfigFile("/tmp/group-mapping.json");
        options.CreatUserGroupMapping();
        
        assertEquals(options.getUserIdFromUserName("user1"), "101");
        assertEquals(options.getUserIdFromUserName("user2"), "102");
        assertEquals(options.getUserNameFromUserId("101"), "user1");
        assertEquals(options.getUserNameFromUserId("102"), "user2");
           
        assertEquals(options.getGroupIdFromGroupName("group1"), "201");
        assertEquals(options.getGroupIdFromGroupName("group2"), "202");
        assertEquals(options.getGroupNameFromGroupId("201"), "group1");
        assertEquals(options.getGroupNameFromGroupId("202"), "group2");
    }
}
