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
import java.util.Scanner;
import java.util.function.Consumer;

import static com.mongodb.client.model.Indexes.ascending;
import static com.mongodb.client.model.Indexes.descending;

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
                            .append("age", nextLine[1])
                            .append("courses", "[" + nextLine[2] + "]");
                    // Вставляем документ в коллекцию
                    collection.insertOne(student);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
//        collection.find().forEach((Consumer<Document>) document -> {
//            System.out.println(">" + document);
//        });

        System.out.println("— общее количество студентов в базе:  " + collection.countDocuments());
        BsonDocument query = BsonDocument.parse("{age: {$gt: 40}}");
        System.out.println("— количество студентов старше 40 лет: "
                + collection.countDocuments(query));

        collection.find(new Document("age", new Document("$gt", 40)))
                .forEach((Consumer<Document>) document
                        -> System.out.println("студент старше сорока:\n" + document.get("name")));

        FindIterable<Document> doc = collection.find()
                .sort(ascending("age"))
                .limit(1);
        doc.forEach((Consumer<Document>) document -> {
            System.out.println("— имя самого молодого студента: " + document.get("name"));
        });
        doc = collection.find()
                .sort(descending("age"))
                .limit(1);
        doc.forEach((Consumer<Document>) document -> {
            System.out.println("— список курсов самого старого студента: " + document.get("courses"));
        });
    }
}

//— общее количество студентов в базе.
//
//— количество студентов старше 40 лет.
//
//— имя самого молодого студента.
//
//— список курсов самого старого студента.