package gov.ornl.stucco.DBClient;

import gov.ornl.stucco.DBClient.DBConnection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.json.JSONException;
import org.json.JSONObject;

import com.tinkerpop.rexster.client.RexProException;
import com.tinkerpop.rexster.client.RexsterClient;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class DBConnectionTest 
extends TestCase
{
	private static final int WAIT_TIME = 3;
	
	/**
	 * Create the test case
	 *
	 * @param testName name of the test case
	 */
	public DBConnectionTest( String testName )
	{
		super( testName );
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite()
	{
		return new TestSetup(new TestSuite(DBConnectionTest.class)) {

	        protected void setUp() throws Exception {
	            System.out.println(" Global setUp started");
	            DBConnection c = null;
	    		try{
	    			RexsterClient client = DBConnection.createClient(DBConnection.getTestConfig(), WAIT_TIME);
	    			c = new DBConnection( client );
	    			c.createIndices();
	    		}catch(Exception e){
	    			e.printStackTrace(); //TODO
	    		} //don't really care
	    		System.out.println(" Global setUp done");
	        }
	        protected void tearDown() throws Exception {

	        }
	    };
	}



	/**
	 * Tests loading, querying, and other basic operations for vertices, edges, properties.
	 * @throws IOException 
	 * @throws RexProException 
	 * @throws JSONException 
	 */
	public void testLoad() throws JSONException, RexProException, IOException
	{
		DBConnection c = null;
		try{
			Configuration config = DBConnection.getTestConfig();
			RexsterClient client = DBConnection.createClient(config, WAIT_TIME);
			c = new DBConnection( client );
			c.createIndices();
		}catch(Exception e){
			e.printStackTrace(); //TODO
		} 

		c.removeAllVertices(); // when all vertices are removed all edges are dropped

		String vert1 = "{" +
				"\"_id\":\"CVE-1999-0002\"," +
				"\"_type\":\"vertex\","+
				"\"source\":[\"CVE\"],"+
				"\"description\":\"Buffer overflow in NFS mountd gives root access to remote attackers, mostly in Linux systems.\","+
				"\"references\":["+
				"\"CERT:CA-98.12.mountd\","+
				"\"http://www.ciac.org/ciac/bulletins/j-006.shtml\","+
				"\"http://www.securityfocus.com/bid/121\","+
				"\"XF:linux-mountd-bo\"],"+
				"\"status\":\"Entry\","+
				"\"score\":1.0"+
				"}";
		String vert2 = "{"+
				"\"_id\":\"CVE-1999-nnnn\"," +
				"\"_type\":\"vertex\","+
				"\"source\":[\"CVE\"],"+
				"\"description\":\"test description asdf.\","+
				"\"references\":["+
				"\"http://www.google.com\"],"+
				"\"status\":\"Entry\","+
				"\"score\":1.0"+
				"}";
		c.addVertexFromJSON(new JSONObject(vert1));
		c.addVertexFromJSON(new JSONObject(vert2));
		
		try {
			//find this node, check some properties.
			String id = c.findVertId("CVE-1999-0002");
			Map<String, Object> query_ret_map = c.getVertByID(id);
			String[] expectedRefs = {"CERT:CA-98.12.mountd","XF:linux-mountd-bo","http://www.ciac.org/ciac/bulletins/j-006.shtml","http://www.securityfocus.com/bid/121"};
			String[] actualRefs = ((ArrayList<String>)query_ret_map.get("references")).toArray(new String[0]);
			assertTrue(expectedRefs.length == actualRefs.length);
			Arrays.sort(expectedRefs);
			Arrays.sort(actualRefs);
			assertTrue(Arrays.equals(expectedRefs, actualRefs));

			//find the other node, check its properties.
			String id2 = c.findVertId("CVE-1999-nnnn");
			query_ret_map = (Map<String,Object>)c.findVert("CVE-1999-nnnn").get("_properties");
			assertEquals("test description asdf.", query_ret_map.get("description"));
			expectedRefs = new String[]{"http://www.google.com"};
			actualRefs = ((ArrayList<String>)query_ret_map.get("references")).toArray(new String[0]);
			assertTrue(Arrays.equals(expectedRefs, actualRefs));

			//There should be no edge between them
			assertEquals(0, c.getEdgeCount(id, id2, "sameAs"));
			assertEquals(0, c.getEdgeCount(id2, id, "sameAs")); //just to be sure.
			
		} catch (RexProException e) {
			fail("RexProException");
			e.printStackTrace();
		} catch (IOException e) {
			fail("IOException");
			e.printStackTrace();
		}
		
		String edge = "{"+ 
				"\"_id\":\"asdf\"," +
				"\"_inV\":\"CVE-1999-0002\"," +
				"\"_outV\":\"CVE-1999-nnnn\"," +
				"\"_label\":\"sameAs\","+
				"\"description\":\"some_description\""+
				"}";
		c.addEdgeFromJSON(new JSONObject(edge));

		try {
			
			//and now we can test the edge between them
			String id = c.findVertId("CVE-1999-0002");
			String id2 = c.findVertId("CVE-1999-nnnn");
			assertEquals(1, c.getEdgeCount(id, id2, "sameAs"));
			assertEquals(0, c.getEdgeCount(id2, id, "sameAs")); //just to be sure.
			Object query_ret;
			query_ret = c.getClient().execute("g.v(\""+id2+"\").outE().inV();");
			List<Map<String, Object>> query_ret_list = (List<Map<String, Object>>)query_ret;
			Map<String, Object> query_ret_map = query_ret_list.get(0);
			assertEquals(id, query_ret_map.get("_id"));

			c.removeAllVertices();

		} catch (RexProException e) {
			fail("RexProException");
			e.printStackTrace();
		} catch (IOException e) {
			fail("IOException");
			e.printStackTrace();
		}
	}


	/**
	 * Tests updating vertex properties
	 * @throws IOException 
	 * @throws RexProException 
	 */

	public void testUpdate() throws RexProException, IOException
	{
		DBConnection c = null;
		try{
			RexsterClient client = DBConnection.createClient(DBConnection.getTestConfig(), WAIT_TIME);
			c = new DBConnection( client );
			c.createIndices();
		}catch(Exception e){
			e.printStackTrace(); //TODO
		}

		c.removeAllVertices();
		
		String vert1 = "{"+
                "\"endIPInt\":55," +
                "\"_type\":\"vertex\","+
                "\"source\": [\"aaaa\"],"+
                "\"name\":\"testvert_55\"" +
                "}";
        c.addVertexFromJSON(new JSONObject(vert1));
		
		String id = c.findVertId("testvert_55");
		
		c.updateVertProperty(id, "source", "aaaa");
		
		Map<String, Object> query_ret_map = c.getVertByID(id);
		assertEquals( "55", query_ret_map.get("endIPInt").toString());
		assertEquals( "[aaaa]", query_ret_map.get("source").toString());

		Map<String, Object> newProps = new HashMap<String, Object>();
		newProps.put("startIPInt", "33");
		newProps.put("endIPInt", "44");
		newProps.put("source", "bbbb");
		c.updateVert(id, newProps);

		query_ret_map = c.getVertByID(id);
		assertEquals("33", query_ret_map.get("startIPInt").toString());
		assertEquals("44", query_ret_map.get("endIPInt").toString());
		assertEquals("[aaaa, bbbb]", query_ret_map.get("source").toString());
		
		newProps = new HashMap<String, Object>();
		String[] sourceArray = {"cccc", "dddd"};
		newProps.put("source", sourceArray);
		c.updateVert(id, newProps);

		query_ret_map = c.getVertByID(id);
		assertEquals("[aaaa, bbbb, cccc, dddd]", query_ret_map.get("source").toString());
		
		newProps = new HashMap<String, Object>();
		Set<String> sourceSet = new HashSet<String>();
		sourceSet.add("eeee");
		sourceSet.add("ffff");
		newProps.put("source", sourceSet);
		c.updateVert(id, newProps);

		query_ret_map = c.getVertByID(id);
		assertEquals("[aaaa, bbbb, cccc, dddd, eeee, ffff]", query_ret_map.get("source").toString());
		
		newProps = new HashMap<String, Object>();
		List<String> sourceList = new ArrayList<String>();
		sourceList.add("gggg");
		sourceList.add("hhhh");
		newProps.put("source", sourceList);
		c.updateVert(id, newProps);

		query_ret_map = c.getVertByID(id);
		assertEquals("[aaaa, bbbb, cccc, dddd, eeee, ffff, gggg, hhhh]", query_ret_map.get("source").toString());
		
		newProps = new HashMap<String, Object>();
		String[] sourceArr = new String[]{"gggg", "hhhh"};
		newProps.put("source", sourceArr);
		c.updateVert(id, newProps);

		query_ret_map = c.getVertByID(id);
		assertEquals("[aaaa, bbbb, cccc, dddd, eeee, ffff, gggg, hhhh]", query_ret_map.get("source").toString());

		c.removeAllVertices();
	}

	/**
	 * creates a vertex of high reverse degree, and one of low degree, and searches for the edge(s) between them.
	 * @throws IOException 
	 * @throws RexProException 
	 * @throws JSONException 
	 */
	public void testHighForwardDegreeVerts() throws JSONException, RexProException, IOException
	{
		DBConnection c = null;
		try{
			Configuration config = DBConnection.getTestConfig();
			RexsterClient client = DBConnection.createClient(config, WAIT_TIME);
			c = new DBConnection( client );
			c.createIndices();
		}catch(Exception e){
			e.printStackTrace(); //TODO
		} 

		c.removeAllVertices();

		String vert1 = "{" +
				"\"_id\":\"/usr/local/something\"," +
				"\"_type\":\"vertex\","+
				"\"source\":\"test\","+
				"\"vertexType\":\"software\""+
				"}";
		String vert2 = "{" +
				"\"_id\":\"11.11.11.11:1111_to_22.22.22.22:1\"," +
				"\"_type\":\"vertex\","+
				"\"source\":\"test\","+
				"\"vertexType\":\"flow\""+
				"}";
		c.addVertexFromJSON(new JSONObject(vert1));
		c.addVertexFromJSON(new JSONObject(vert2));

		try {
			//find node ids
			String id = c.findVertId("/usr/local/something");
			String id2 = c.findVertId("11.11.11.11:1111_to_22.22.22.22:1");

			//There should be no edge between them
			assertEquals(0, c.getEdgeCount(id, id2, "hasFlow")); //just to be sure.
			assertEquals(0, c.getEdgeCount(id2, id, "hasFlow"));

		} catch (RexProException e) {
			fail("RexProException");
			e.printStackTrace();
		} catch (IOException e) {
			fail("IOException");
			e.printStackTrace();
		}

		String edge = "{"+ 
				"\"_id\":\"/usr/local/something_hasFlow_11.11.11.11:1111_to_22.22.22.22:1\"," +
				"\"_inV\":\"11.11.11.11:1111_to_22.22.22.22:1\"," +
				"\"_outV\":\"/usr/local/something\"," +
				"\"_label\":\"hasFlow\","+
				"\"description\":\"test edge\""+
				"}";
		c.addEdgeFromJSON(new JSONObject(edge));

		try {
			//find node ids
			String id = c.findVertId("/usr/local/something");
			String id2 = c.findVertId("11.11.11.11:1111_to_22.22.22.22:1");

			//There should be no edge between them
			assertEquals(0, c.getEdgeCount(id, id2, "hasFlow")); //just to be sure.
			assertEquals(1, c.getEdgeCount(id2, id, "hasFlow"));

		} catch (RexProException e) {
			fail("RexProException");
			e.printStackTrace();
		} catch (IOException e) {
			fail("IOException");
			e.printStackTrace();
		}

		for(int i=2; i<800; i++){
			String currentVert = "{" +
					"\"_id\":\"11.11.11.11:1111_to_22.22.22.22:" + i + "\"," +
					"\"_type\":\"vertex\","+
					"\"source\":\"test\","+
					"\"vertexType\":\"flow\""+
					"}";
			c.addVertexFromJSON(new JSONObject(currentVert));

			String currentEdge = "{"+ 
					"\"_id\":\"/usr/local/something_hasFlow_11.11.11.11:1111_to_22.22.22.22:" + i + "\"," +
					"\"_inV\":\"11.11.11.11:1111_to_22.22.22.22:" + i + "\"," +
					"\"_outV\":\"/usr/local/something\"," +
					"\"_label\":\"hasFlow\","+
					"\"description\":\"test edge\""+
					"}";
			c.addEdgeFromJSON(new JSONObject(currentEdge));
		}

		try {
			//find node ids
			String id = c.findVertId("/usr/local/something");
			String id2 = c.findVertId("11.11.11.11:1111_to_22.22.22.22:1");

			//There should be no edge between them
			assertEquals(0, c.getEdgeCount(id, id2, "hasFlow")); //just to be sure.
			assertEquals(1, c.getEdgeCount(id2, id, "hasFlow"));

		} catch (RexProException e) {
			fail("RexProException");
			e.printStackTrace();
		} catch (IOException e) {
			fail("IOException");
			e.printStackTrace();
		}
	}

	/**
	 * creates a vertex of high reverse degree, and one of low degree, and searches for the edge(s) between them.
	 * @throws IOException 
	 * @throws RexProException 
	 * @throws JSONException 
	 */
	public void testHighReverseDegreeVerts() throws JSONException, RexProException, IOException
	{
		DBConnection c = null;
		try{
			Configuration config = DBConnection.getTestConfig();
			RexsterClient client = DBConnection.createClient(config, WAIT_TIME);
			c = new DBConnection( client );
			c.createIndices();
		}catch(Exception e){
			e.printStackTrace(); //TODO
		}

		c.removeAllVertices();

		String vert1 = "{" +
				"\"_id\":\"11.11.11.11:1111\"," +
				"\"_type\":\"vertex\","+
				"\"source\":\"test\","+
				"\"vertexType\":\"address\""+
				"}";
		String vert2 = "{" +
				"\"_id\":\"11.11.11.11\"," +
				"\"_type\":\"vertex\","+
				"\"source\":\"test\","+
				"\"vertexType\":\"IP\""+
				"}";
		c.addVertexFromJSON(new JSONObject(vert1));
		c.addVertexFromJSON(new JSONObject(vert2));

		try {
			//find node ids
			String id = c.findVertId("11.11.11.11:1111");
			String id2 = c.findVertId("11.11.11.11");

			//There should be no edge between them
			assertEquals(0, c.getEdgeCount(id, id2, "hasIP"));
			assertEquals(0, c.getEdgeCount(id2, id, "hasIP")); //just to be sure.

		} catch (RexProException e) {
			fail("RexProException");
			e.printStackTrace();
		} catch (IOException e) {
			fail("IOException");
			e.printStackTrace();
		}

		String edge = "{"+ 
				"\"_id\":\"11.11.11.11:1111_hasIP_11.11.11.11\"," +
				"\"_inV\":\"11.11.11.11\"," +
				"\"_outV\":\"11.11.11.11:1111\"," +
				"\"_label\":\"hasIP\","+
				"\"description\":\"test edge\""+
				"}";
		c.addEdgeFromJSON(new JSONObject(edge));

		try {
			//find node ids
			String id = c.findVertId("11.11.11.11:1111");
			String id2 = c.findVertId("11.11.11.11");

			//There should be no edge between them
			assertEquals(0, c.getEdgeCount(id, id2, "hasIP"));
			assertEquals(1, c.getEdgeCount(id2, id, "hasIP")); //just to be sure.

		} catch (RexProException e) {
			fail("RexProException");
			e.printStackTrace();
		} catch (IOException e) {
			fail("IOException");
			e.printStackTrace();
		}

		for(int i=1200; i<2000; i++){
			String currentVert = "{" +
					"\"_id\":\"11.11.11.11:" + i + "\"," +
					"\"_type\":\"vertex\","+
					"\"source\":\"test\","+
					"\"vertexType\":\"address\""+
					"}";
			c.addVertexFromJSON(new JSONObject(currentVert));

			String currentEdge = "{"+ 
					"\"_id\":\"11.11.11.11:" + i + "_hasIP_11.11.11.11\"," +
					"\"_inV\":\"11.11.11.11\"," +
					"\"_outV\":\"11.11.11.11:" + i + "\"," +
					"\"_label\":\"hasIP\","+
					"\"description\":\"test edge\""+
					"}";
			c.addEdgeFromJSON(new JSONObject(currentEdge));
		}

		try {
			//find node ids
			String id = c.findVertId("11.11.11.11:1111");
			String id2 = c.findVertId("11.11.11.11");

			//There should be no edge between them
			assertEquals(0, c.getEdgeCount(id, id2, "hasIP"));
			assertEquals(1, c.getEdgeCount(id2, id, "hasIP")); //just to be sure.

		} catch (RexProException e) {
			fail("RexProException");
			e.printStackTrace();
		} catch (IOException e) {
			fail("IOException");
			e.printStackTrace();
		}
	}
}


