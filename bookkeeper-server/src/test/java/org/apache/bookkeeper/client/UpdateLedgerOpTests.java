package org.apache.bookkeeper.client;

import org.apache.bookkeeper.bookie.BookieShell;
import org.apache.bookkeeper.net.BookieId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.apache.bookkeeper.client.util.UpdateLedgerNotifier;
import org.mockito.Mockito;


import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;



@RunWith(value = Enclosed.class)
public class UpdateLedgerOpTests {
    @RunWith(Parameterized.class)
    public static class UpdateBookieIdInLedgerTests{
    private final boolean expectedException;
    private final String oldBookieId;
    private final String newBookieId;
    private int rate;
    int maxOutstandingReads;
    int limit;
    private final BookieShell.UpdateLedgerNotifier progressable;
    private final UpdateLedgerOp updateLedgerOp;
    @Parameterized.Parameters

    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{
                { "id001", "id001", -1, -1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", -1, -1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", -1, -1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", -1, -1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", -1, -1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", -1, -1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", -1, 0, -1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", -1, 0, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", -1, 0, 0, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", -1, 0, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", -1, 0, 1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", -1, 0, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", -1, 1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", -1, 1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", -1, 1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", -1, 1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", -1, 1, 1, new UpdateLedgerNotifier(5,2), true},
                { "id001", "id001", -1, 1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", 0, -1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", 0, -1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", 0, -1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", 0, -1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", 0, -1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", 0, -1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", 0, 0, -1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", 0, 0, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", 0, 0, 0, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", 0, 0, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", 0, 0, 1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", 0, 0, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", 0, 1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", 0, 1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", 0, 1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", 0, 1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", 0, 1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", 0, 1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", 1, -1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", 1, -1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", 1, -1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", 1, -1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", 1, -1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", 1, -1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", 1, 0, -1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", 1, 0, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", 1, 0, 0, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", 1, 0, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", 1, 0, 1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", 1, 0, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", 1, 1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", 1, 1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", 1, 1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id001", 1, 1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id001", 1, 1, 1, new UpdateLedgerNotifier(5,2), false },
                { "id001", "id001", 1, 1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", -1, -1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", -1, -1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", -1, -1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", -1, -1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", -1, -1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", -1, -1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", -1, 0, -1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", -1, 0, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", -1, 0, 0, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", -1, 0, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", -1, 0, 1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", -1, 0, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", -1, 1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", -1, 1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", -1, 1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", -1, 1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", -1, 1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", -1, 1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", 0, -1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", 0, -1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", 0, -1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", 0, -1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", 0, -1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", 0, -1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", 0, 0, -1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", 0, 0, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", 0, 0, 0, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", 0, 0, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", 0, 0, 1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", 0, 0, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", 0, 1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", 0, 1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", 0, 1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", 0, 1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", 0, 1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", 0, 1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", 1, -1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", 1, -1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", 1, -1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", 1, -1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", 1, -1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", 1, -1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", 1, 0, -1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", 1, 0, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", 1, 0, 0, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", 1, 0, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", 1, 0, 1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", 1, 0, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", 1, 1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", 1, 1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", 1, 1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", 1, 1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id001", "id$001", 1, 1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id001", "id$001", 1, 1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", -1, -1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", -1, -1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", -1, -1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", -1, -1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", -1, -1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", -1, -1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", -1, 0, -1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", -1, 0, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", -1, 0, 0, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", -1, 0, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", -1, 0, 1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", -1, 0, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", -1, 1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", -1, 1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", -1, 1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", -1, 1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", -1, 1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", -1, 1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", 0, -1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", 0, -1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", 0, -1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", 0, -1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", 0, -1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", 0, -1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", 0, 0, -1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", 0, 0, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", 0, 0, 0, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", 0, 0, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", 0, 0, 1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", 0, 0, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", 0, 1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", 0, 1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", 0, 1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", 0, 1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", 0, 1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id$001","id001", 0, 1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", 1, -1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", 1, -1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", 1, -1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", 1, -1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", 1, -1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", 1, -1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", 1, 0, -1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", 1, 0, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", 1, 0, 0, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", 1, 0, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", 1, 0, 1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", 1, 0, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", 1, 1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", 1, 1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", 1, 1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", 1, 1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id001", 1, 1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id001", 1, 1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", -1, -1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", -1, -1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", -1, -1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", -1, -1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", -1, -1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", -1, -1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", -1, 0, -1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", -1, 0, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", -1, 0, 0, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", -1, 0, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", -1, 0, 1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", -1, 0, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", -1, 1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", -1, 1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", -1, 1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", -1, 1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", -1, 1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", -1, 1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", 0, -1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", 0, -1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", 0, -1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", 0, -1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", 0, -1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", 0, -1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", 0, 0, -1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", 0, 0, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", 0, 0, 0, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", 0, 0, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", 0, 0, 1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", 0, 0, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", 0, 1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", 0, 1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", 0, 1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", 0, 1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", 0, 1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", 0, 1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", 1, -1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", 1, -1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", 1, -1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", 1, -1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", 1, -1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", 1, -1, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", 1, 0, -1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", 1, 0, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", 1, 0, 0, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", 1, 0, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", 1, 0, 1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", 1, 0, 1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", 1, 1, -1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", 1, 1, -1, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", 1, 1, 0, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", 1, 1, 0, new UpdateLedgerNotifier(-5,-2), true },
                { "id$001", "id$001", 1, 1, 1, new UpdateLedgerNotifier(5,2), true },
                { "id$001", "id$001", 1, 1, 1, new UpdateLedgerNotifier(-5,-2), true }
        });
    }
    public UpdateBookieIdInLedgerTests(String oldBookieId, String newBookieId,
                               int rate, int maxOutstandingReads, int limit,
                               BookieShell.UpdateLedgerNotifier progressable, boolean exc){
        BookKeeper bookKeeperMock= Mockito.mock(BookKeeper.class);
        BookKeeperAdmin bookKeeperAdminMock= Mockito.mock(BookKeeperAdmin.class);
        this.updateLedgerOp= new UpdateLedgerOp(bookKeeperMock,bookKeeperAdminMock );

        this.oldBookieId=oldBookieId;
        this.newBookieId=newBookieId;
        this.limit=limit;
        this.rate=rate;
        this.maxOutstandingReads=maxOutstandingReads;
        this.progressable=progressable;
        this.expectedException= exc;


    }
    @Test
    public void UpdateBookieIdInLedgerTest(){

        try{
            this.updateLedgerOp.updateBookieIdInLedgers(BookieId.parse(this.oldBookieId),BookieId.parse(this.newBookieId), this.limit, this.maxOutstandingReads, this.rate,this.progressable);

        }catch(IOException e){
            Assert.assertTrue(this.expectedException);
        }catch(InterruptedException e){
            Assert.assertTrue(this.expectedException);
        }catch(IllegalArgumentException e){
            Assert.assertTrue(this.expectedException);
        }
    }
    }

}
