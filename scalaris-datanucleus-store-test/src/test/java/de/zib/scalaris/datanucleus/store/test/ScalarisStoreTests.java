package de.zib.scalaris.datanucleus.store.test;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
    TestScalarisStorage.class,
    TestScalarisQuery.class
})
public class ScalarisStoreTests {
	
}
