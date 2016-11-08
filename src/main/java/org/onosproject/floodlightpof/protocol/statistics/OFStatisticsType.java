/**
 *    modified by hdy
 **/

package org.onosproject.floodlightpof.protocol.statistics;

import org.onosproject.floodlightpof.protocol.Instantiable;
import org.onosproject.floodlightpof.protocol.OFType;

import java.lang.reflect.Constructor;

public enum OFStatisticsType {
    DESC(0, OFDescriptionStatistics.class, OFDescriptionStatistics.class,
            new OFDescriptionStatisticsInstantiable(),
            new OFDescriptionStatisticsInstantiable()),
    FLOW(1, OFFlowStatisticsRequest.class, OFFlowStatisticsReply.class,
            new OFFlowStatisticsRequestInstantiable(),
            new OFFlowStatisticsReplyInstantiable()),
    AGGREGATE(2, OFAggregateStatisticsRequest.class, OFAggregateStatisticsReply.class,
            new OFAggregateStatisticsRequestInstantiable(),
            new OFAggregateStatisticsReplyInstantiable()),
    TABLE(3, OFTableStatistics.class, OFTableStatistics.class,
            new OFTableStatisticsInstantiable(),
            new OFTableStatisticsInstantiable()),
    PORT(4, OFPortStatisticsRequest.class, OFPortStatisticsReply.class,
            new OFPortStatisticsRequestInstantiable(),
            new OFPortStatisticsReplyInstantiable()),
    QUEUE(5, OFQueueStatisticsRequest.class, OFQueueStatisticsReply.class,
            new OFQueueStatisticsRequestInstantiable(),
            new OFQueueStatisticsReplyInstantiable()),
    VENDOR(0xffff, OFVendorStatistics.class, OFVendorStatistics.class,
            new OFVendorStatisticsInstantiable(),
            new OFVendorStatisticsInstantiable());

    static OFStatisticsType[] requestMapping;
    static OFStatisticsType[] replyMapping;

    protected Class<? extends OFStatistics> requestClass;
    protected Constructor<? extends OFStatistics> requestConstructor;
    protected Instantiable<OFStatistics> requestInstantiable;
    protected Class<? extends OFStatistics> replyClass;
    protected Constructor<? extends OFStatistics> replyConstructor;
    protected Instantiable<OFStatistics> replyInstantiable;
    protected short type;

    /**
     * Store some information about the OpenFlow Statistic type, including wire
     * protocol type number, and derived class.
     *
     * @param type Wire protocol number associated with this OFStatisticsType
     * @param requestClass The Statistics Java class to return when the
     *                     containing OFType is STATS_REQUEST
     * @param replyClass   The Statistics Java class to return when the
     *                     containing OFType is STATS_REPLY
     */
    OFStatisticsType(int type, Class<? extends OFStatistics> requestClass,
                     Class<? extends OFStatistics> replyClass,
                     Instantiable<OFStatistics> requestInstantiable,
                     Instantiable<OFStatistics> replyInstantiable) {
        this.type = (short) type;
        this.requestClass = requestClass;
        try {
            this.requestConstructor = requestClass.getConstructor(new Class[]{});
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failure getting constructor for class: " + requestClass, e);
        }

        this.replyClass = replyClass;
        try {
            this.replyConstructor = replyClass.getConstructor(new Class[]{});
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failure getting constructor for class: " + replyClass, e);
        }
        this.requestInstantiable = requestInstantiable;
        this.replyInstantiable = replyInstantiable;
        OFStatisticsType.addMapping(this.type, OFType.MULTIPART_REQUEST, this);
        OFStatisticsType.addMapping(this.type, OFType.MULTIPART_REPLY, this);
    }

    /**
     * Adds a mapping from type value to OFStatisticsType enum.
     *
     * @param i OpenFlow wire protocol type
     * @param t type of containing OFMessage, only accepts STATS_REQUEST or
     *          STATS_REPLY
     * @param st type
     */
    public static void addMapping(short i, OFType t, OFStatisticsType st) {
        if (i < 0) {
            i = (short) (16 + i);
        }
        if (t == OFType.MULTIPART_REQUEST) {
            if (requestMapping == null) {
                requestMapping = new OFStatisticsType[16];
            }
            OFStatisticsType.requestMapping[i] = st;
        } else if (t == OFType.MULTIPART_REPLY) {
            if (replyMapping == null) {
                replyMapping = new OFStatisticsType[16];
            }
            OFStatisticsType.replyMapping[i] = st;
        } else {
            throw new RuntimeException(t.toString() + " is an invalid OFType");
        }
    }

    /**
     * Remove a mapping from type value to OFStatisticsType enum.
     *
     * @param i OpenFlow wire protocol type
     * @param t type of containing OFMessage, only accepts STATS_REQUEST or
     *          STATS_REPLY
     */
    public static void removeMapping(short i, OFType t) {
        if (i < 0) {
            i = (short) (16 + i);
        }
        if (t == OFType.MULTIPART_REQUEST) {
            requestMapping[i] = null;
        } else if (t == OFType.MULTIPART_REPLY) {
            replyMapping[i] = null;
        } else {
            throw new RuntimeException(t.toString() + " is an invalid OFType");
        }
    }

    /**
     * Given a wire protocol OpenFlow type number, return the OFStatisticsType
     * associated with it.
     *
     * @param i wire protocol number
     * @param t type of containing OFMessage, only accepts STATS_REQUEST or
     *          STATS_REPLY
     * @return OFStatisticsType enum type
     */
    public static OFStatisticsType valueOf(short i, OFType t) {
        if (i < 0) {
            i = (short) (16 + i);
        }
        if (t == OFType.MULTIPART_REQUEST) {
            return requestMapping[i];
        } else if (t == OFType.MULTIPART_REPLY) {
            return replyMapping[i];
        } else {
            throw new RuntimeException(t.toString() + " is an invalid OFType");
        }
    }

    /**
     * @return Returns the wire protocol value corresponding to this
     * OFStatisticsType
     */
    public short getTypeValue() {
        return this.type;
    }

    /**
     * @param t type of containing OFMessage, only accepts STATS_REQUEST or
     *          STATS_REPLY
     * @return return the OFMessage subclass corresponding to this
     *                OFStatisticsType
     */
    public Class<? extends OFStatistics> toClass(OFType t) {
        if (t == OFType.MULTIPART_REQUEST) {
            return requestClass;
        } else if (t == OFType.MULTIPART_REPLY) {
            return replyClass;
        } else {
            throw new RuntimeException(t.toString() + " is an invalid OFType");
        }
    }

    /**
     * Returns the no-argument Constructor of the implementation class for
     * this OFStatisticsType, either request or reply based on the supplied.
     * OFType
     *
     * @param t
     * @return constructor
     */
    public Constructor<? extends OFStatistics> getConstructor(OFType t) {
        if (t == OFType.MULTIPART_REQUEST) {
            return requestConstructor;
        } else if (t == OFType.MULTIPART_REPLY) {
            return replyConstructor;
        } else {
            throw new RuntimeException(t.toString() + " is an invalid OFType");
        }
    }

    /**
     * @return the requestInstantiable
     */
    public Instantiable<OFStatistics> getRequestInstantiable() {
        return requestInstantiable;
    }

    /**
     * @param requestInstantiable the requestInstantiable to set
     */
    public void setRequestInstantiable(
            Instantiable<OFStatistics> requestInstantiable) {
        this.requestInstantiable = requestInstantiable;
    }

    /**
     * @return the replyInstantiable
     */
    public Instantiable<OFStatistics> getReplyInstantiable() {
        return replyInstantiable;
    }

    /**
     * @param replyInstantiable the replyInstantiable to set
     */
    public void setReplyInstantiable(Instantiable<OFStatistics> replyInstantiable) {
        this.replyInstantiable = replyInstantiable;
    }

    /**
     * Returns a new instance of the implementation class for
     * this OFStatisticsType, either request or reply based on the supplied
     * OFType.
     *
     * @param t
     * @return new instance
     */
    public OFStatistics newInstance(OFType t) {
        if (t == OFType.MULTIPART_REQUEST) {
            return requestInstantiable.instantiate();
        } else if (t == OFType.MULTIPART_REPLY) {
            return replyInstantiable.instantiate();
        } else {
            throw new RuntimeException(t.toString() + " is an invalid OFType");
        }
    }
    static class OFDescriptionStatisticsInstantiable implements  Instantiable<OFStatistics> {
        @Override
        public OFStatistics instantiate() {
            return new OFDescriptionStatistics();
        }
    }

    static class OFFlowStatisticsRequestInstantiable implements  Instantiable<OFStatistics> {
        @Override
        public OFStatistics instantiate() {
            return new OFFlowStatisticsRequest();
        }
    }

    static class OFFlowStatisticsReplyInstantiable implements  Instantiable<OFStatistics> {
        @Override
        public OFStatistics instantiate() {
            return new OFFlowStatisticsReply();
        }
    }


    static class OFAggregateStatisticsRequestInstantiable implements  Instantiable<OFStatistics> {
        @Override
        public OFStatistics instantiate() {
            return new OFAggregateStatisticsRequest();
        }
    }

    static class OFAggregateStatisticsReplyInstantiable implements  Instantiable<OFStatistics> {
        @Override
        public OFStatistics instantiate() {
            return new OFAggregateStatisticsReply();
        }
    }


    static class OFTableStatisticsInstantiable implements  Instantiable<OFStatistics> {
        @Override
        public OFStatistics instantiate() {
            return new OFTableStatistics();
        }
    }

    static class OFPortStatisticsRequestInstantiable implements  Instantiable<OFStatistics> {
        @Override
        public OFStatistics instantiate() {
            return new OFPortStatisticsRequest();
        }
    }


    static class OFPortStatisticsReplyInstantiable implements  Instantiable<OFStatistics> {
        @Override
        public OFStatistics instantiate() {
            return new OFPortStatisticsReply();
        }
    }

    static class OFQueueStatisticsRequestInstantiable implements  Instantiable<OFStatistics> {
        @Override
        public OFStatistics instantiate() {
            return new OFQueueStatisticsRequest();
        }
    }


    static class OFQueueStatisticsReplyInstantiable implements  Instantiable<OFStatistics> {
        @Override
        public OFStatistics instantiate() {
            return new OFQueueStatisticsReply();
        }
    }

    static class OFVendorStatisticsInstantiable implements  Instantiable<OFStatistics> {
        @Override
        public OFStatistics instantiate() {
            return new OFVendorStatistics();
        }
    }
}
