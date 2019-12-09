package sequence;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.*;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

public class SequenceCreate {

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    /**
     * This procedure defined in this class, it is used to create an indexed time-series
     * with the nodes representing the steps values given a frequency
     *
     * The name of the label is related to the frequency this acyclic graph will be set,
     * and its id field will store the value on each related step.
     *
     * @param size the size of the graph with respect of number of nodes/steps
     * @param frequency in terms of Hertz, 600 Hz, i.e.
     * @param relName a list of property keys to index, only the ones the node
     *                 actually contains will be added
     */
    @Procedure(value = "sequence.createseqsignal", mode=Mode.WRITE)
    @Description("For the size, frequency, node and relationship names a time-series with steps linked will be created")
    public void createseqsignal( @Name("size") long size,
                           @Name("frequency") long frequency,
                           @Name("relName") String relName) {


        MathContext MATH_CTX = new MathContext(6, RoundingMode.HALF_UP);

        BigDecimal freqcycles = new BigDecimal((1d)/((double)frequency), MATH_CTX);

        String createnodes =
                            "WITH range(1, " + size + ") AS freq " +
                            "FOREACH(f in freq | CREATE (:Fs" + frequency + " {id:f * " + freqcycles + "}));";

        Map<String, Object> paramseq = new HashMap<>();
        paramseq.put("relName", relName);

        String createseq =
        "MATCH (fs:Fs" + frequency + ") " +
        "WITH fs " +
        "ORDER BY fs.id " +
        "WITH collect(fs) as fss " +
        "FOREACH(i in RANGE(0, length(fss)-2) | " +
        "        FOREACH(fs1 in [fss[i]] | " +
        "                FOREACH(fs2 in [fss[i+1]] | " +
        "                        CREATE UNIQUE (fs1)-[:{relName}]->(fs2))))";

        String createindex = "CREATE INDEX ON :Fs" + frequency + "(id);";

        //if Neo4j Enterprise Edition
        //String constraint = "CREATE CONSTRAINT ON (fs:Fs" + frequency + ") ASSERT exists(fs.id)";

        try (Transaction transact = db.beginTx()) {
            db.execute(createnodes);
            db.execute(createseq, paramseq);
            db.execute(createindex);
            transact.success();
        } catch (Throwable ex) {
            log.error("SequenceCreate:createseqsignal - Error on creating sequence of signal: `%s` | `%s` | `%s` | ", createnodes, createseq, createindex, ex.toString());
            //tail -10 /var/log/neo4j/debug.log | grep "SequenceCreate:createseqsignal"
            throw new TransactionFailureException("SequenceCreate:createseqsignal - Failed on creating time-series.", ex);
        }
    }
}
