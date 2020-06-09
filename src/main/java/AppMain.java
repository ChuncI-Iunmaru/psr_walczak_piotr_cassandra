import com.datastax.oss.driver.api.core.CqlSession;

public class AppMain {

    public static void main(String[] args) {
        try (CqlSession session = CqlSession.builder().build()) {
            KeyspaceBuilderManager keyspaceManager = new KeyspaceBuilderManager(session, "liga");
            keyspaceManager.dropKeyspace();
            keyspaceManager.selectKeyspaces();
            keyspaceManager.createKeyspace();
            keyspaceManager.useKeyspace();

            MatchTableManager matchTableManager = new MatchTableManager(session);
            //matchTableManager.dropTable();
            matchTableManager.createMatchTable();
            matchTableManager.insertMatch();
            matchTableManager.getAll();
        }
    }
}
