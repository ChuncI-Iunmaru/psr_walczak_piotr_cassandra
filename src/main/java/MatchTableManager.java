import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateType;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import javafx.util.Pair;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MatchTableManager extends SimpleManager {
    final private static Random r = new Random(System.currentTimeMillis());
    public MatchTableManager(CqlSession session) {
        super(session);
    }

     public void createMatchTable() {
        CreateType createType = SchemaBuilder.createType("goal").withField("team", DataTypes.TEXT)
                .withField("time", DataTypes.INT).withField("player", DataTypes.TEXT);

        session.execute(createType.build());

        CreateTable createTable = SchemaBuilder.createTable("match")
                .withPartitionKey("id", DataTypes.INT)
                .withColumn("date", DataTypes.TEXT)
                .withColumn("stadium", DataTypes.TEXT)
                .withColumn("team1", DataTypes.TEXT)
                .withColumn("team2", DataTypes.TEXT)
                .withColumn("score1", DataTypes.INT)
                .withColumn("score2", DataTypes.INT)
                .withColumn("goals", DataTypes.frozenListOf(QueryBuilder.udt("goal")));
        session.execute(createTable.build());
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
                .value("id", QueryBuilder.raw(Integer.toString(key)))
                .value("date", QueryBuilder.raw(String.format("'%s'", date)))
                .value("stadium", QueryBuilder.raw(String.format("'%s'", stadium)))
                .value("team1", QueryBuilder.raw(String.format("'%s'", firstTeam)))
                .value("team2", QueryBuilder.raw(String.format("'%s'", secondTeam)))
                .value("score1", QueryBuilder.raw(result.getKey().toString()))
                .value("score2", QueryBuilder.raw(result.getValue().toString()))
                .value("goals", QueryBuilder.raw(goals.toString()));
        System.out.println(insert);
        session.execute(insert.build());
    }

    public void getAll() {
        Select query = QueryBuilder.selectFrom("match").all();
        SimpleStatement statement = query.build();
        ResultSet resultSet = session.execute(statement);
        for (Row row : resultSet) {
            ConsoleUtils.printRowAsMatch(row);
        }
    }

    public void dropTable() {
        Drop drop = SchemaBuilder.dropTable("match");
        executeSimpleStatement(drop.build());
    }
}
