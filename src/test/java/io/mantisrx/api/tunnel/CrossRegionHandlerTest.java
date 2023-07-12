package io.mantisrx.api.tunnel;

import com.google.common.collect.ImmutableList;
import io.mantisrx.api.push.ConnectionBroker;
import junit.framework.TestCase;
import org.junit.Test;
import rx.Scheduler;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class CrossRegionHandlerTest extends TestCase {

    @Test
    public void testParseUriRegion() {

        CrossRegionHandler regionHandler = spy(new CrossRegionHandler(ImmutableList.of(), mock(MantisCrossRegionalClient.class), mock(ConnectionBroker.class), mock(Scheduler.class)));
        doReturn(ImmutableList.of("us-east-1", "eu-west-1")).when(regionHandler).getTunnelRegions();

        assertEquals(ImmutableList.of("us-east-1"), regionHandler.parseRegionsInUri("/region/us-east-1/foobar"));
        assertEquals(ImmutableList.of("us-east-2"), regionHandler.parseRegionsInUri("/region/us-east-2/foobar"));
        assertEquals(ImmutableList.of("us-east-1", "eu-west-1"), regionHandler.parseRegionsInUri("/region/all/foobar"));
        assertEquals(ImmutableList.of("us-east-1", "eu-west-1"), regionHandler.parseRegionsInUri("/region/ALL/foobar"));

        doReturn(ImmutableList.of("us-east-1", "eu-west-1", "us-west-2")).when(regionHandler).getTunnelRegions();
        assertEquals(ImmutableList.of("us-east-1", "eu-west-1", "us-west-2"), regionHandler.parseRegionsInUri("/region/ALL/foobar"));

        assertEquals(ImmutableList.of("us-east-1", "us-east-2"), regionHandler.parseRegionsInUri("/region/us-east-1,us-east-2/foobar"));
        assertEquals(ImmutableList.of("us-east-1", "us-west-2"), regionHandler.parseRegionsInUri("/region/us-east-1,us-west-2/foobar"));
    }
}
