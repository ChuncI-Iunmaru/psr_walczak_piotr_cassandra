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
import com.datastax.oss.driver.api.querybuilder.update.Update;
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
                .withColumn("time", DataTypes.INT)
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
        int matchTime = ConsoleUtils.getNumber(90, -1);
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
                .value("time", QueryBuilder.literal(matchTime))
                .value("goals", QueryBuilder.raw(goals.toString()));

        insertIntoSecondary(firstTeam, key, result.getKey());
        insertIntoSecondary(secondTeam, key, result.getValue());
        //For debug
        System.out.println(insert);
        session.execute(insert.build());
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
        //System.out.println("Nie zaimplementowane");
        System.out.println("Pobieranie zapytaniem");
        System.out.println("Podaj nazwę drużyny: ");
        String team = ConsoleUtils.getText(1);
        Select query = QueryBuilder.selectFrom("team_scores").all().whereColumn("team").isEqualTo(QueryBuilder.literal(team));
        SimpleStatement statement = query.build();
        ResultSet resultSet = session.execute(statement);
        if (resultSet.getAvailableWithoutFetching() == 0) {
            System.out.println("Drużyna nie rozegrała żadnych meczy w lidze!");
            return;
        }
        List<Integer> ids = new ArrayList<>();
        for (Row row: resultSet) {
            ids.add(row.getInt("match_id"));
        }
        Select selectMatchesById = QueryBuilder.selectFrom("match").all().whereColumn("id").in(QueryBuilder.bindMarker());
        PreparedStatement preparedStatement = session.prepare(selectMatchesById.build());
        resultSet = session.execute(preparedStatement.bind(ids));
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
        System.out.println("Podaj id meczu do usunięcia:");
        int id = ConsoleUtils.getNumber(-1, 0, -1);
        Row matchToDelete = getOneMatchById(id);
        if (matchToDelete == null) return;
        Delete deleteFromMain = QueryBuilder.deleteFrom("match").whereColumn("id").isEqualTo(QueryBuilder.literal(id));

        deleteFromSecondary(matchToDelete.getString("team1"), id);
        deleteFromSecondary(matchToDelete.getString("team2"), id);

        session.execute(deleteFromMain.build());
    }

    public void updateMatch() {
        System.out.println("Podaj id meczu do aktualizacji:");
        int id = ConsoleUtils.getNumber(-1, 0, -1);
        Row matchToUpdate = getOneMatchById(id);
        if (matchToUpdate == null) return;
        String date = ConsoleUtils.getFormattedDate(matchToUpdate.getString("date"));
        System.out.println("Podaj nazwę obiektu. Obecna wartość: "+matchToUpdate.getString("stadium")+". Pozostaw puste by nie zmieniać.");
        String stadium = ConsoleUtils.getText(0);
        if (stadium.isEmpty()) stadium = matchToUpdate.getString("stadium");
        System.out.println("Podaj nazwę pierwszej drużyny. Obecna wartość: "+matchToUpdate.getString("team1")+". Pozostaw puste by nie zmieniać.");
        String firstTeam = ConsoleUtils.getText(0);
        if (firstTeam.isEmpty()) firstTeam = matchToUpdate.getString("team1");
        System.out.println("Podaj nazwę drugiej drużyny. Obecna wartość: "+matchToUpdate.getString("team2")+". Pozostaw puste by nie zmieniać.");
        String secondTeam = ConsoleUtils.getText(0);
        if (secondTeam.isEmpty()) secondTeam = matchToUpdate.getString("team2");
        System.out.println("Podaj czas trwania meczu. Obecna wartość: "+matchToUpdate.getInt("time")+". Pozostaw puste by nie zmieniać.");
        int matchTime = ConsoleUtils.getNumber(matchToUpdate.getInt("time"), 90, -1);
        System.out.println("Po wprowadzeniu zmian podaj nowy wynik i gole:");
        Pair<Integer, Integer> result = ConsoleUtils.getScores(firstTeam, secondTeam);
        List<Goal> goals = Stream.concat(
                ConsoleUtils.getGoalsForTeam(result.getKey(), matchTime, firstTeam).stream(),
                ConsoleUtils.getGoalsForTeam(result.getValue(), matchTime, secondTeam).stream()
        ).collect(Collectors.toList());

        Update update = QueryBuilder.update("match")
                .setColumn("date", QueryBuilder.literal(date))
                .setColumn("stadium", QueryBuilder.literal(stadium))
                .setColumn("team1", QueryBuilder.literal(firstTeam))
                .setColumn("team2", QueryBuilder.literal(secondTeam))
                .setColumn("score1", QueryBuilder.literal(result.getKey()))
                .setColumn("score2", QueryBuilder.literal(result.getValue()))
                .setColumn("time", QueryBuilder.literal(matchTime))
                .setColumn("goals", QueryBuilder.raw(goals.toString()))
                .whereColumn("id").isEqualTo(QueryBuilder.literal(id));

        deleteFromSecondary(matchToUpdate.getString("team1"), id);
        deleteFromSecondary(matchToUpdate.getString("team2"), id);
        insertIntoSecondary(firstTeam, id, result.getKey());
        insertIntoSecondary(secondTeam, id, result.getValue());

        session.execute(update.build());
    }

    public void calculateTeamStats() {
        System.out.println("Przetwarzanie danych");
        System.out.println("Podaj nazwę drużyny: ");
        String team = ConsoleUtils.getText(1);
        Select query = QueryBuilder.selectFrom("team_scores").all().whereColumn("team").isEqualTo(QueryBuilder.literal(team));
        SimpleStatement statement = query.build();
        ResultSet resultSet = session.execute(statement);
        if (resultSet.getAvailableWithoutFetching() == 0) {
            System.out.println("Drużyna nie rozegrała żadnych meczy w lidze!");
            return;
        }
        int matches=0;
        double sum_goals=0;
        int lowest=999999;
        int highest=-1;
        for (Row row : resultSet) {
            matches+=1;
            int score = row.getInt("score");
            sum_goals+=score;
            if (score < lowest) lowest = score;
            if (score > highest) highest = score;
        }
        System.out.println("Drużyna rozegrała w lidze "+matches+" meczy i zdobyła "+sum_goals+" gol(i)");
        System.out.println("Najwięcej goli w meczu: "+highest);
        System.out.println("Najmniej goli w meczu: "+lowest);
        System.out.println("średnio goli w meczu: "+sum_goals/matches);
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

    private Row getOneMatchById(int id) {
        Select select = QueryBuilder.selectFrom("match").all()
                .whereColumn("id").isEqualTo(QueryBuilder.literal(id));
        ResultSet resultSet = session.execute(select.build());
        if (resultSet.getAvailableWithoutFetching() == 0){
            System.out.println("Nie znaleziono meczu o takim id.");
            return null;
        }
        return resultSet.one();
    }

    private void insertIntoSecondary(String team, int match_id, int score) {
        Insert insert = QueryBuilder.insertInto("liga", "team_scores")
                .value("team", QueryBuilder.raw(String.format("'%s'", team)))
                .value("match_id", QueryBuilder.raw(Integer.toString(match_id)))
                .value("score", QueryBuilder.raw(Integer.toString(score)));
        session.execute(insert.build());
    }

    private void deleteFromSecondary(String team, int match_id) {
        Delete delete = QueryBuilder.deleteFrom("team_scores")
                .whereColumn("team").isEqualTo(QueryBuilder.literal(team))
                .whereColumn("match_id").isEqualTo(QueryBuilder.literal(match_id));
        session.execute(delete.build());
    }
}
