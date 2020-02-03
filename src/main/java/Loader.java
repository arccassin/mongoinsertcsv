import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVReader;
import org.bson.BsonDocument;
import org.bson.Document;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;

import static com.mongodb.client.model.Indexes.ascending;
import static com.mongodb.client.model.Indexes.descending;
import static java.util.Arrays.asList;

/**
 * Created by User on 30 Янв., 2020
 */
public class Loader {

    public static void main(String[] args) {

        MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);
        MongoDatabase database = mongoClient.getDatabase("local");
        // Создаем коллекцию
        MongoCollection<Document> collection = database.getCollection("Skill_Students");

        // Удалим из нее все документы
        collection.drop();

        Scanner scanner = new Scanner(System.in);
        String filePath = scanner.nextLine();
        CSVReader reader;

        {
            try {
                reader = new CSVReader(new FileReader(filePath));
                String[] nextLine;
                while ((nextLine = reader.readNext()) != null) {
                    Document student = new Document()
                            .append("name", nextLine[0])
                            .append("age", Integer.parseInt(nextLine[1]))
                            .append("courses", Arrays.asList(nextLine[2]));
                    // Вставляем документ в коллекцию
                    collection.insertOne(student);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.println("— общее количество студентов в базе:  " + collection.countDocuments());
        BsonDocument query = BsonDocument.parse("{age: {$gt: 40}}");
        System.out.println("— количество студентов старше 40 лет: "
                + collection.countDocuments(query));

        FindIterable<Document> doc = collection.find()
                .sort(ascending("age"))
                .limit(1);
        doc.forEach((Consumer<Document>) document -> {
            System.out.println("— имя самого молодого студента: " + document.get("name"));
        });

        List<Document> docs =  collection.find()
                .sort(descending("age"))
                .limit(1)
                .into(new ArrayList<>());
        Document student = docs.get(0);
        List<String> courses = new ArrayList<>();
        courses = student.getList("courses", String.class);
        System.out.print("— список курсов самого старого студента: ");
        for (int i = 0; i < courses.size(); i++) {
            System.out.print(courses.get(i));
            System.out.print(i != courses.size() - 1 ? ", " : "");
        }
    }
}

//— общее количество студентов в базе.
//
//— количество студентов старше 40 лет.
//
//— имя самого молодого студента.
//
//— список курсов самого старого студента.