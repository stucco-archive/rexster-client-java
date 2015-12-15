package gov.ornl.stucco.DBClient;

import gov.ornl.stucco.DBClient.InMemoryDBConnection;
import gov.ornl.stucco.DBClient.Constraint.Condition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class InMemoryDBConnectionTest 
extends TestCase
{
	
	/**
	 * Create the test case
	 *
	 * @param testName name of the test case
	 */
	public InMemoryDBConnectionTest( String testName )
	{
		super( testName );
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite()
	{
		//return new TestSuite( DBConnectionTest.class );
		return new TestSetup(new TestSuite(InMemoryDBConnectionTest.class)) {

	        protected void setUp() throws Exception {
	            //System.out.println(" Global setUp started");
	    		//System.out.println(" Global setUp done");
	        }
	        protected void tearDown() throws Exception {
	            //System.out.println(" Global tearDown ");
	        }
	    };
	}



	public void testConstraints() throws Exception
	{
		InMemoryDBConnection conn = new InMemoryDBConnection();
		Map<String, Object> vert;
		List<Constraint> constraints;
		String id;
		List<String> ids;
		List<String> expectedIds;
		
		vert = new HashMap<String, Object>();
		vert.put("name", "aaa_5");
		vert.put("aaa", 5);
		conn.addVertex(vert);
		
		vert = new HashMap<String, Object>();
		vert.put("name", "aaa_6");
		vert.put("aaa", 6);
		conn.addVertex(vert);
		
		vert = new HashMap<String, Object>();
		vert.put("name", "aaa_7");
		vert.put("aaa", 7);
		conn.addVertex(vert);
		
		Constraint c1 = new Constraint("aaa", Condition.eq, new Integer(5) );
		constraints = new LinkedList<Constraint>();
		constraints.add(c1);
		ids = conn.getVertIDsByConstraints(constraints);
		assertEquals(1, ids.size());
		assertTrue(ids.contains(conn.getVertIDByName("aaa_5")));
		//System.out.println("Found " + ids.size() + " matching verts with aaa == 5");
		
		c1 = new Constraint("aaa", Condition.neq, new Integer(5) );
		constraints = new LinkedList<Constraint>();
		constraints.add(c1);
		ids = conn.getVertIDsByConstraints(constraints);
		expectedIds = new LinkedList<String>();
		expectedIds.add(conn.getVertIDByName("aaa_6"));
		expectedIds.add(conn.getVertIDByName("aaa_7"));
		assertTrue(ids.containsAll(expectedIds));
		assertEquals(2, ids.size());
		//System.out.println("Found " + ids.size() + " matching verts with aaa != 5");
		
		c1 = new Constraint("aaa", Condition.gt, new Integer(5) );
		constraints = new LinkedList<Constraint>();
		constraints.add(c1);
		ids = conn.getVertIDsByConstraints(constraints);
		expectedIds = new LinkedList<String>();
		expectedIds.add(conn.getVertIDByName("aaa_6"));
		expectedIds.add(conn.getVertIDByName("aaa_7"));
		assertTrue(ids.containsAll(expectedIds));
		assertEquals(2, ids.size());
		//System.out.println("Found " + ids.size() + " matching verts with aaa > 5");
		
		c1 = new Constraint("aaa", Condition.gte, new Integer(5) );
		constraints = new LinkedList<Constraint>();
		constraints.add(c1);
		ids = conn.getVertIDsByConstraints(constraints);
		expectedIds = new LinkedList<String>();
		expectedIds.add(conn.getVertIDByName("aaa_5"));
		expectedIds.add(conn.getVertIDByName("aaa_6"));
		expectedIds.add(conn.getVertIDByName("aaa_7"));
		assertTrue(ids.containsAll(expectedIds));
		assertEquals(3, ids.size());
		//System.out.println("Found " + ids.size() + " matching verts with aaa >= 5");
		
		c1 = new Constraint("aaa", Condition.lt, new Integer(6) );
		constraints = new LinkedList<Constraint>();
		constraints.add(c1);
		ids = conn.getVertIDsByConstraints(constraints);
		assertTrue(ids.contains(conn.getVertIDByName("aaa_5")));
		assertEquals(1, ids.size());
		//System.out.println("Found " + ids.size() + " matching verts with aaa < 6");
		
		c1 = new Constraint("aaa", Condition.lte, new Integer(6) );
		constraints = new LinkedList<Constraint>();
		constraints.add(c1);
		ids = conn.getVertIDsByConstraints(constraints);
		expectedIds = new LinkedList<String>();
		expectedIds.add(conn.getVertIDByName("aaa_5"));
		expectedIds.add(conn.getVertIDByName("aaa_6"));
		assertTrue(ids.containsAll(expectedIds));
		assertEquals(2, ids.size());
		//System.out.println("Found " + ids.size() + " matching verts with aaa <= 6");
		
		vert = new HashMap<String, Object>();
		vert.put("name", "bbb_4_5_6");
		vert.put("bbb", (new int[] {4,5,6}) );
		conn.addVertex(vert);
		
		vert = new HashMap<String, Object>();
		vert.put("name", "bbb_Integer_4_5_6");
		vert.put("bbb", (new Integer[] {new Integer(4), new Integer(5), new Integer(6)}) );
		conn.addVertex(vert);
		
		vert = new HashMap<String, Object>();
		vert.put("name", "bbb_7_8_9");
		vert.put("bbb", (new int[] {7,8,9}) );
		conn.addVertex(vert);
		
		vert = new HashMap<String, Object>();
		vert.put("name", "bbb_5_6_7_8");
		vert.put("bbb", (new int[] {5,6,7,8}) );
		conn.addVertex(vert);
		
		vert = new HashMap<String, Object>();
		vert.put("name", "bbb_asdf");
		vert.put("bbb", "asdf" );
		conn.addVertex(vert);
		
		vert = new HashMap<String, Object>();
		vert.put("name", "bbb_asdf4.222");
		vert.put("bbb", "asdf4.222" );
		conn.addVertex(vert);
		
		vert = new HashMap<String, Object>();
		vert.put("name", "bbb_55");
		vert.put("bbb", 55 );
		conn.addVertex(vert);
		
		vert = new HashMap<String, Object>();
		vert.put("name", "bbb_101_102_103");
		vert.put("bbb", (new double[] {101, 102, 103.0}) );
		conn.addVertex(vert);
		
		c1 = new Constraint("bbb", Condition.in, 4);
		constraints = new LinkedList<Constraint>();
		constraints.add(c1);
		ids = conn.getVertIDsByConstraints(constraints);
		expectedIds = new LinkedList<String>();
		expectedIds.add(conn.getVertIDByName("bbb_4_5_6"));
		expectedIds.add(conn.getVertIDByName("bbb_Integer_4_5_6"));
		assertTrue(ids.containsAll(expectedIds));
		assertEquals(2, ids.size());
		//System.out.println("Found " + ids.size() + " matching verts with 4 in bbb");
		
		c1 = new Constraint("bbb", Condition.in, new Integer(4));
		constraints = new LinkedList<Constraint>();
		constraints.add(c1);
		ids = conn.getVertIDsByConstraints(constraints);
		expectedIds = new LinkedList<String>();
		expectedIds.add(conn.getVertIDByName("bbb_4_5_6"));
		expectedIds.add(conn.getVertIDByName("bbb_Integer_4_5_6"));
		assertTrue(ids.containsAll(expectedIds));
		assertEquals(2, ids.size());
		//System.out.println("Found " + ids.size() + " matching verts with (Integer)4 in bbb");
		
		c1 = new Constraint("bbb", Condition.in, 4.0);
		constraints = new LinkedList<Constraint>();
		constraints.add(c1);
		ids = conn.getVertIDsByConstraints(constraints);
		assertEquals(0, ids.size());
		//System.out.println("Found " + ids.size() + " matching verts with 4.0 in bbb");
		
		c1 = new Constraint("bbb", Condition.in, 4.2);
		constraints = new LinkedList<Constraint>();
		constraints.add(c1);
		ids = conn.getVertIDsByConstraints(constraints);
		assertEquals(0, ids.size());
		//System.out.println("Found " + ids.size() + " matching verts with 4.2 in bbb");
		
		c1 = new Constraint("bbb", Condition.notin, 4);
		constraints = new LinkedList<Constraint>();
		constraints.add(c1);
		ids = conn.getVertIDsByConstraints(constraints);
		expectedIds = new LinkedList<String>();
		expectedIds.add(conn.getVertIDByName("bbb_7_8_9"));
		expectedIds.add(conn.getVertIDByName("bbb_5_6_7_8"));
		expectedIds.add(conn.getVertIDByName("bbb_asdf"));
		expectedIds.add(conn.getVertIDByName("bbb_asdf4.222"));
		expectedIds.add(conn.getVertIDByName("bbb_55"));
		expectedIds.add(conn.getVertIDByName("bbb_101_102_103"));
		assertTrue(ids.containsAll(expectedIds));
		assertEquals(6, ids.size());
		//System.out.println("Found " + ids.size() + " matching verts without 4 in bbb");
		
		c1 = new Constraint("bbb", Condition.in, 5);
		Constraint c2 = new Constraint("bbb", Condition.in, 7);
		constraints = new LinkedList<Constraint>();
		constraints.add(c1);
		constraints.add(c2);
		ids = conn.getVertIDsByConstraints(constraints);
		expectedIds = new LinkedList<String>();
		expectedIds.add(conn.getVertIDByName("bbb_5_6_7_8"));
		assertTrue(ids.containsAll(expectedIds));
		assertEquals(1, ids.size());
		//System.out.println("Found " + ids.size() + " matching verts with 5 and 7 in bbb");
		
		c1 = new Constraint("bbb", Condition.in, 'a');
		constraints = new LinkedList<Constraint>();
		constraints.add(c1);
		ids = conn.getVertIDsByConstraints(constraints);
		expectedIds = new LinkedList<String>();
		expectedIds.add(conn.getVertIDByName("bbb_asdf"));
		expectedIds.add(conn.getVertIDByName("bbb_asdf4.222"));
		assertTrue(ids.containsAll(expectedIds));
		assertEquals(2, ids.size());
		//System.out.println("Found " + ids.size() + " matching verts with 'a' in bbb");
		
		c1 = new Constraint("bbb", Condition.in, "as");
		constraints = new LinkedList<Constraint>();
		constraints.add(c1);
		ids = conn.getVertIDsByConstraints(constraints);
		expectedIds = new LinkedList<String>();
		expectedIds.add(conn.getVertIDByName("bbb_asdf"));
		expectedIds.add(conn.getVertIDByName("bbb_asdf4.222"));
		assertTrue(ids.containsAll(expectedIds));
		assertEquals(2, ids.size());
		//System.out.println("Found " + ids.size() + " matching verts with \"as\" in bbb");
		
		c1 = new Constraint("bbb", Condition.in, 101);
		constraints = new LinkedList<Constraint>();
		constraints.add(c1);
		ids = conn.getVertIDsByConstraints(constraints);
		assertEquals(0, ids.size());
		//System.out.println("Found " + ids.size() + " matching verts with 101 in bbb");
		
		c1 = new Constraint("bbb", Condition.in, 103);
		constraints = new LinkedList<Constraint>();
		constraints.add(c1);
		ids = conn.getVertIDsByConstraints(constraints);
		assertEquals(0, ids.size());
		//System.out.println("Found " + ids.size() + " matching verts with 103 in bbb");
		
	}



}


