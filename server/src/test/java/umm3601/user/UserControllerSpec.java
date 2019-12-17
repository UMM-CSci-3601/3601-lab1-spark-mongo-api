package umm3601.user;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import org.bson.*;
import org.bson.codecs.*;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.json.JsonReader;
import org.bson.types.ObjectId;


import java.util.*;
import java.util.stream.Collectors;


import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;


public class UserControllerSpec {
  private UserController userController;
  private ObjectId samsId;

  static MongoClient mongoClient;
  static MongoDatabase db;

  @BeforeAll
  public static void setupDB() {
    int mongoPort = Integer.parseInt(System.getProperty("test.mongo.port"));

    mongoClient = MongoClients.create(
      MongoClientSettings.builder()
      .applyToClusterSettings(builder ->
      builder.hosts(Arrays.asList(new ServerAddress("localhost", mongoPort))))
      .build());

      db = mongoClient.getDatabase("test");
  }

  @AfterAll
  public static void teardown() {
    db.drop();
    mongoClient.close();
  }

  @BeforeEach
  public void clearAndPopulateDB() {
    MongoCollection<Document> userDocuments = db.getCollection("users");
    userDocuments.drop();
    List<Document> testUsers = new ArrayList<>();
    testUsers.add(Document.parse("{\n" +
      "                    name: \"Chris\",\n" +
      "                    age: 25,\n" +
      "                    company: \"UMM\",\n" +
      "                    email: \"chris@this.that\"\n" +
      "                }"));
    testUsers.add(Document.parse("{\n" +
      "                    name: \"Pat\",\n" +
      "                    age: 37,\n" +
      "                    company: \"IBM\",\n" +
      "                    email: \"pat@something.com\"\n" +
      "                }"));
    testUsers.add(Document.parse("{\n" +
      "                    name: \"Jamie\",\n" +
      "                    age: 37,\n" +
      "                    company: \"Frogs, Inc.\",\n" +
      "                    email: \"jamie@frogs.com\"\n" +
      "                }"));

    samsId = new ObjectId();
    BasicDBObject sam = new BasicDBObject("_id", samsId);
    sam = sam.append("name", "Sam")
      .append("age", 45)
      .append("company", "Frogs, Inc.")
      .append("email", "sam@frogs.com");


    userDocuments.insertMany(testUsers);
    userDocuments.insertOne(Document.parse(sam.toJson()));

    // It might be important to construct this _after_ the DB is set up
    // in case there are bits in the constructor that care about the state
    // of the database.
    userController = new UserController(db);
  }

  // http://stackoverflow.com/questions/34436952/json-parse-equivalent-in-mongo-driver-3-x-for-java
  private BsonArray parseJsonArray(String json) {
    final CodecRegistry codecRegistry
      = CodecRegistries.fromProviders(Arrays.asList(
      new ValueCodecProvider(),
      new BsonValueCodecProvider(),
      new DocumentCodecProvider()));

    JsonReader reader = new JsonReader(json);
    BsonArrayCodec arrayReader = new BsonArrayCodec(codecRegistry);

    return arrayReader.decode(reader, DecoderContext.builder().build());
  }

  private static String getName(BsonValue val) {
    BsonDocument doc = val.asDocument();
    return ((BsonString) doc.get("name")).getValue();
  }

  @Test
  public void getAllUsers() {
    Map<String, String[]> emptyMap = new HashMap<>();
    String jsonResult = userController.getUsers(emptyMap);
    BsonArray docs = parseJsonArray(jsonResult);

    assertEquals(4, docs.size(), "Should be 4 users");
    List<String> names = docs
      .stream()
      .map(UserControllerSpec::getName)
      .sorted()
      .collect(Collectors.toList());
    List<String> expectedNames = Arrays.asList("Chris", "Jamie", "Pat", "Sam");
    assertEquals(expectedNames, names, "Names should match");
  }

  @Test
  public void getUsersWhoAre37() {
    Map<String, String[]> argMap = new HashMap<>();
    argMap.put("age", new String[]{"37"});
    String jsonResult = userController.getUsers(argMap);
    BsonArray docs = parseJsonArray(jsonResult);

    assertEquals(2, docs.size(),"Should be 2 users");
    List<String> names = docs
      .stream()
      .map(UserControllerSpec::getName)
      .sorted()
      .collect(Collectors.toList());
    List<String> expectedNames = Arrays.asList("Jamie", "Pat");
    assertEquals(expectedNames, names, "Names should match");
  }

  @Test
  public void getSamById() {
    String jsonResult = userController.getUser(samsId.toHexString());
    Document sam = Document.parse(jsonResult);
    assertEquals("Sam", sam.get("name"), "Name should match");
    String noJsonResult = userController.getUser(new ObjectId().toString());
    assertNull(noJsonResult, "No name should match");

  }

  @Test
  public void addUserTest() {
    String newId = userController.addNewUser("Brian", 22, "umm", "brian@yahoo.com");

    assertNotNull(newId, "Add new user should return true when user is added");
    Map<String, String[]> argMap = new HashMap<>();
    argMap.put("age", new String[]{"22"});
    String jsonResult = userController.getUsers(argMap);
    BsonArray docs = parseJsonArray(jsonResult);

    List<String> name = docs
      .stream()
      .map(UserControllerSpec::getName)
      .sorted()
      .collect(Collectors.toList());
    assertEquals("Brian", name.get(0), "Should return name of new user");
  }

  @Test
  public void getUserByCompany() {
    Map<String, String[]> argMap = new HashMap<>();
    //Mongo in UserController is doing a regex search so can just take a Java Reg. Expression
    //This will search the company starting with an I or an F
    argMap.put("company", new String[]{"[I,F]"});
    String jsonResult = userController.getUsers(argMap);
    BsonArray docs = parseJsonArray(jsonResult);
    assertEquals(3, docs.size(), "Should be 3 users");
    List<String> name = docs
      .stream()
      .map(UserControllerSpec::getName)
      .sorted()
      .collect(Collectors.toList());
    List<String> expectedName = Arrays.asList("Jamie", "Pat", "Sam");
    assertEquals(expectedName, name, "Names should match");

  }


}
