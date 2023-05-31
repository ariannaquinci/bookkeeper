import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.DigestType;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.when;

public class BookKeeperTest {

    @Test
    public void testCreateLedger() throws InterruptedException, BKException {
        // Crea un mock di BookKeeper
        BookKeeper bookKeeperMock = Mockito.mock(BookKeeper.class);

        // Configura i parametri di input per il metodo createLedger
        int ensSize = 10;
        int writeQuorumSize = 3;
        int ackQuorumSize = 3;
        DigestType digestType = DigestType.CRC32;
        byte[] passwd = "password".getBytes();

        // Crea un mock di LedgerHandle da restituire dal metodo createLedger
        LedgerHandle expectedLedgerHandle = Mockito.mock(LedgerHandle.class);

        // Configura il comportamento del mock
        when(bookKeeperMock.createLedger(ensSize, writeQuorumSize, ackQuorumSize, BookKeeper.DigestType.fromApiDigestType(digestType), passwd))
                .thenReturn(expectedLedgerHandle);

        // Chiamata al metodo under test
        LedgerHandle actualLedgerHandle = bookKeeperMock.createLedger(ensSize, writeQuorumSize, ackQuorumSize,
                BookKeeper.DigestType.fromApiDigestType(digestType), passwd);

        // Verifica che il mock abbia restituito il valore atteso
        assertEquals(expectedLedgerHandle, actualLedgerHandle);

          }
}
