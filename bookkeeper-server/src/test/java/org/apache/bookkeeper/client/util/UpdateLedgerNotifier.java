package org.apache.bookkeeper.client.util;

import org.apache.bookkeeper.bookie.BookieShell;
import org.apache.bookkeeper.common.util.MathUtils;

import java.util.concurrent.TimeUnit;

import static org.apache.bookkeeper.net.NetworkTopologyImpl.LOG;

public class UpdateLedgerNotifier implements BookieShell.UpdateLedgerNotifier {


        long lastReport;
        int printprogress;
        public UpdateLedgerNotifier(long l, int p){
            this.lastReport=l;
            this.printprogress=p;
        }
        @Override
        public void progress(long updated, long issued) {
            if (TimeUnit.MILLISECONDS.toSeconds(MathUtils.elapsedMSec(lastReport)) >= printprogress) {
                LOG.info("Number of ledgers issued={}, updated={}", issued, updated);
                lastReport = MathUtils.nowInNano();
            }
        }

}
