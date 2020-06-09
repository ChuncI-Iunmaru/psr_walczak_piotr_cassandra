import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.datastax.oss.driver.api.querybuilder.schema.CreateType;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MatchTableManager extends SimpleManager {
    final private static Random r = new Random(System.currentTimeMillis());

    public MatchTableManager(CqlSession session) {
        super(session);
    }

     public void createTables() {
        CreateType createType = SchemaBuilder.createType("goal").ifNotExists().withField("team", DataTypes.TEXT)
                .withField("time", DataTypes.INT).withField("player", DataTypes.TEXT);

        session.execute(createType.build());

        CreateTableWithOptions createTable = SchemaBuilder.createTable("match").ifNotExists()
                .withPartitionKey("id", DataTypes.INT)
                .withColumn("date", DataTypes.TEXT)
                .withColumn("stadium", DataTypes.TEXT)
                .withColumn("team1", DataTypes.TEXT)
                .withColumn("team2", DataTypes.TEXT)
                .withColumn("score1", DataTypes.INT)
                .withColumn("score2", DataTypes.INT)
                .withColumn("goals", DataTypes.frozenListOf(QueryBuilder.udt("goal")));
        CreateTableWithOptions createSecondaryTable = SchemaBuilder.createTable("team_scores").ifNotExists()
                .withPartitionKey("team", DataTypes.TEXT)
                .withClusteringColumn("match_id", DataTypes.INT)
                .withColumn("score", DataTypes.INT).withClusteringOrder("match_id", ClusteringOrder.ASC);
        session.execute(createTable.build());
        session.execute(createSecondaryTable.build());
    }

    public void insertMatch() {
        String date = ConsoleUtils.getFormattedDate();
        System.out.println("Podaj nazwę obiektu: ");
        String stadium = ConsoleUtils.getText(1);
        System.out.println("Podaj nazwę pierwszej drużyny:");
        String firstTeam = ConsoleUtils.getText(1);
        System.out.println("Podaj nazwę drugiej drużyny:");
        String secondTeam = ConsoleUtils.getText(1);
        System.out.println("Podaj czas trwania meczu:");
        int matchTime = ConsoleUtils.getNumber(1, -1);
        Pair<Integer, Integer> result = ConsoleUtils.getScores(firstTeam, secondTeam);
        List<Goal> goals = Stream.concat(
                ConsoleUtils.getGoalsForTeam(result.getKey(), matchTime, firstTeam).stream(),
                ConsoleUtils.getGoalsForTeam(result.getValue(), matchTime, secondTeam).stream()
        ).collect(Collectors.toList());
        //System.out.println(goals.toString());
        int key = Math.abs(r.nextInt());
        Insert insert = QueryBuilder.insertInto("liga", "match")
                .value("date", QueryBuilder.raw(String.format("'%s'", date)))
                .value("id", QueryBuilder.raw(Integer.toString(key)))
                .value("stadium", QueryBuilder.raw(String.format("'%s'", stadium)))
                .value("team1", QueryBuilder.raw(String.format("'%s'", firstTeam)))
                .value("team2", QueryBuilder.raw(String.format("'%s'", secondTeam)))
                .value("score1", QueryBuilder.raw(result.getKey().toString()))
                .value("score2", QueryBuilder.raw(result.getValue().toString()))
                .value("goals", QueryBuilder.raw(goals.toString()));

        Insert insertFirstTeamIntoSecondary = QueryBuilder.insertInto("liga", "team_scores")
                .value("team", QueryBuilder.raw(String.format("'%s'", firstTeam)))
                .value("match_id", QueryBuilder.raw(Integer.toString(key)))
                .value("score", QueryBuilder.raw(result.getKey().toString()));
        Insert insertSecondTeamIntoSecondary = QueryBuilder.insertInto("liga", "team_scores")
                .value("team", QueryBuilder.raw(String.format("'%s'", secondTeam)))
                .value("match_id", QueryBuilder.raw(Integer.toString(key)))
                .value("score", QueryBuilder.raw(result.getValue().toString()));

        //For debug
        System.out.println(insert);
        System.out.println(insertFirstTeamIntoSecondary);
        System.out.println(insertSecondTeamIntoSecondary);

        session.execute(insert.build());
        session.execute(insertFirstTeamIntoSecondary.build());
        session.execute(insertSecondTeamIntoSecondary.build());
    }

    public void getAll() {
        Select query = QueryBuilder.selectFrom("match").all();
        buildAndPrintSelectQuery(query);
    }

    public void getById() {
        System.out.println("Podaj id meczu do wyszukania");
        int id = ConsoleUtils.getNumber(-1, 0, -1);
        Select select = QueryBuilder.selectFrom("match").all()
                .whereColumn("id").isEqualTo(QueryBuilder.literal(id));
        buildAndPrintSelectQuery(select);
    }

    public void getByQuery() {
        System.out.println("Podaj nazwę drużyny: ");
        String team = ConsoleUtils.getText(1);
        Select selectMatchIDs = QueryBuilder.selectFrom("team_scores").column("match_id").whereColumn("team").isEqualTo(QueryBuilder.literal(team));
        SimpleStatement statement = selectMatchIDs.build();
        ResultSet resultSet = session.execute(statement);
        if (resultSet.getAvailableWithoutFetching() == 0) {
            System.out.println("Drużyna nie rozegrała żadnych meczy w lidze!");
            return;
        }
        List<Integer> ids = new ArrayList<>();
        for (Row row: resultSet) {
            ids.add(row.getInt("match_id"));
        }
        Select selectMatches = QueryBuilder.selectFrom("match").all().whereColumn("id").in(QueryBuilder.bindMarker());
        PreparedStatement preparedStatement = session.prepare(selectMatches.toString());
        BoundStatement boundStatement = preparedStatement.bind(ids);
        resultSet = session.execute(boundStatement);
        for (Row row: resultSet) {
            ConsoleUtils.printRowAsMatch(row);
        }
    }

    public void dropTables() {
        Drop drop = SchemaBuilder.dropTable("match");
        Drop dropSecondary = SchemaBuilder.dropTable("team_scores");
        executeSimpleStatement(drop.build());
        executeSimpleStatement(dropSecondary.build());
    }

    public void deleteMatch() {
        System.out.println("Podaj id meczu do usunięcia");
        int id = ConsoleUtils.getNumber(-1, 0, -1);
        Select select = QueryBuilder.selectFrom("match").all()
                .whereColumn("id").isEqualTo(QueryBuilder.literal(id));
        ResultSet resultSet = session.execute(select.build());
        if (resultSet.getAvailableWithoutFetching() == 0){
            System.out.println("Nie znaleziono meczu o takim id.");
        }
        Row matchToDelete = resultSet.one();
        Delete deleteFromMain = QueryBuilder.deleteFrom("match").whereColumn("id").isEqualTo(QueryBuilder.literal(id));
        Delete deleteFirstTeam = QueryBuilder.deleteFrom("team_scores")
                .whereColumn("team").isEqualTo(QueryBuilder.literal(matchToDelete.getString("team1")))
                .whereColumn("match_id").isEqualTo(QueryBuilder.literal(id));
        Delete deleteSecondTeam = QueryBuilder.deleteFrom("team_scores")
                .whereColumn("team").isEqualTo(QueryBuilder.literal(matchToDelete.getString("team2")))
                .whereColumn("match_id").isEqualTo(QueryBuilder.literal(id));
        session.execute(deleteFromMain.build());
        session.execute(deleteFirstTeam.build());
        session.execute(deleteSecondTeam.build());
    }

    private void buildAndPrintSelectQuery(Select query){
        SimpleStatement statement = query.build();
        ResultSet resultSet = session.execute(statement);
        if (resultSet.getAvailableWithoutFetching() == 0) {
            System.out.println("Nie znaleziono danych!");
            return;
        }
        for (Row row : resultSet) {
            ConsoleUtils.printRowAsMatch(row);
        }
    }

    public void calculateTeamStats() {
        System.out.println("Podaj nazwę drużyny: ");
        String team = ConsoleUtils.getText(1);
        Select query = QueryBuilder.selectFrom("team_scores").all().whereColumn("team").isEqualTo(QueryBuilder.literal(team));
        SimpleStatement statement = query.build();
        ResultSet resultSet = session.execute(statement);
        if (resultSet.getAvailableWithoutFetching() == 0) {
            System.out.println("Drużyna nie rozegrała żadnych meczy w lidze!");
            return;
        }
        int matches=0, sum_goals=0, lowest=-1, highest=-1;
        for (Row row : resultSet) {
            matches+=1;
            sum_goals+=row.getInt("score");
        }
        System.out.println("Drużyna rozegrała "+matches+" meczy i zdobyła "+sum_goals+" gol(i)");
    }
}
