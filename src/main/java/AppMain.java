import com.datastax.oss.driver.api.core.CqlSession;

public class AppMain {

    public static void main(String[] args) {
        try (CqlSession session = CqlSession.builder().build()) {
            KeyspaceBuilderManager keyspaceManager = new KeyspaceBuilderManager(session, "liga");
//            keyspaceManager.dropKeyspace();
//            keyspaceManager.selectKeyspaces();
//            keyspaceManager.createKeyspace();
            keyspaceManager.useKeyspace();


            MatchTableManager matchTableManager = new MatchTableManager(session);
//            matchTableManager.createTables();
            System.out.println("Aplikacja na PSR lab 6 - Cassandra");
            System.out.println("Piotr Walczak gr. 1ID22B");
            while (true) {
                switch (ConsoleUtils.getMenuOption()) {
                    case 'd':
                        matchTableManager.insertMatch();
                        break;
                    case 'u':
                        matchTableManager.deleteMatch();
                        break;
                    case 'a':
                        break;
                    case 'i':
                        matchTableManager.getById();
                        break;
                    case 'w':
                        matchTableManager.getAll();
                        break;
                    case 'p':
                        matchTableManager.getByQuery();
                        break;
                    case 'o':
                        matchTableManager.calculateTeamStats();
                        break;
                    case 'z':
                        return;
                    default:
                        System.out.println("Podano nieznaną operację. Spróbuj ponownie.");
                }
            }
        }
    }

}
