/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.xa;

import org.apache.flink.connector.jdbc.JdbcTestBase;
import org.apache.flink.connector.jdbc.templates.BooksTable;
import org.apache.flink.connector.jdbc.templates.round2.TableManaged;

import org.junit.jupiter.api.Test;

import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.Xid;

import java.sql.Connection;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** {@link XaFacadeImpl} tests. */
class JdbcXaFacadeImplTest implements JdbcTestBase {

    private static final BooksTable HELPER_TABLE = new BooksTable("helper");

    @Override
    public List<TableManaged> getManagedTables() {
        return Collections.singletonList(HELPER_TABLE);
    }

    private static final Xid XID =
            new Xid() {
                @Override
                public int getFormatId() {
                    return 0;
                }

                @Override
                public byte[] getGlobalTransactionId() {
                    return "01234".getBytes();
                }

                @Override
                public byte[] getBranchQualifier() {
                    return "56789".getBytes();
                }
            };

    @Test
    void testRecover() throws Exception {
        try (XaFacade f = XaFacadeImpl.fromXaDataSource(getDbMetadata().buildXaDataSource())) {
            f.open();
            assertThat(f.recover()).isEmpty();
            f.start(XID);
            // insert some data to prevent database from ignoring the transaction
            try (Connection c = f.getConnection()) {
                HELPER_TABLE.insertTableTestData(c);
            }
            f.endAndPrepare(XID);
        }
        try (XaFacade f = XaFacadeImpl.fromXaDataSource(getDbMetadata().buildXaDataSource())) {
            f.open();
            Collection<Xid> recovered = f.recover();
            recovered.forEach(f::rollback);
            assertThat(recovered).hasSize(1);
        }
    }

    @Test
    void testClose() throws Exception {
        // some drivers (derby, H2) close both connection on either
        // connection.close/xaConnection.close() call, so use mocks here to:
        // a) prevent closing XA connection from connection.close()
        // b) verify that both connections were closed
        XADataSource xaDataSource = mock(XADataSource.class);
        XAConnection xaConnection = mock(XAConnection.class);
        Connection connection = mock(Connection.class);
        when(xaDataSource.getXAConnection()).thenReturn(xaConnection);
        when(xaConnection.getConnection()).thenReturn(connection);

        try (XaFacade f = XaFacadeImpl.fromXaDataSource(xaDataSource)) {
            f.open();
        }
        verify(connection).close();
        verify(xaConnection).close();
    }
}
