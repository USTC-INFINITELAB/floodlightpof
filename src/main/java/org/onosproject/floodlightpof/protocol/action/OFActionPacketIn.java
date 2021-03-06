/**
 * Copyright (c) 2012, 2013, Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.onosproject.floodlightpof.protocol.action;

import org.jboss.netty.buffer.ChannelBuffer;
import org.onosproject.floodlightpof.util.HexString;

/**
 * packet in with reason. Or say take the packet to controller or cpu.
 * E.g. if a packet dis match any flow entry, could use packetIn to take
 * the packet to controller to let controller to process.
 *
 *
 */
public class OFActionPacketIn extends OFAction {
    public static final int MINIMUM_LENGTH = OFAction.MINIMUM_LENGTH + 8;

    protected int reason;

    public OFActionPacketIn() {
        super.setType(OFActionType.PACKET_IN);
        super.setLength((short) MINIMUM_LENGTH);
    }

    public void readFrom(ChannelBuffer data) {
        super.readFrom(data);
        this.reason = data.readInt();
        data.readBytes(4);
    }

    public void writeTo(ChannelBuffer data) {
        super.writeTo(data);
        data.writeInt(reason);
        data.writeZero(4);
    }

    public String toBytesString() {
        return super.toBytesString() +
                HexString.toHex(reason) +
                HexString.byteZeroEnd(4);
    }

    public String toString() {
        return super.toString() +
                ";rz=0x" + HexString.toHex(reason);
    }

    public int getReason() {
        return reason;
    }

    public void setReason(int reason) {
        this.reason = reason;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + reason;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        OFActionDrop other = (OFActionDrop) obj;
        if (reason != other.reason) {
            return false;
        }
        return true;
    }
}
