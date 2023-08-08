package com.dbmasker.database.elasticsearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class EsClient {

    private String url;

    public EsClient(String host, int port, String protocol) {
        this.url = protocol + "://" + host + ":" + port;
    }

    public void createData() {
        try {
            createIndex();
            insertData();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void cleanData() {
        try {
            String index = "employees";
            URL url = new URL(this.url + "/" + index);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("DELETE");

            int responseCode = connection.getResponseCode();
            System.out.println("Delete Index Response Code: " + responseCode);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void createIndex() throws Exception {
        String index = "employees";
        String indexDefinition = """
                {
                    "mappings": {
                        "properties": {
                            "id": {"type": "integer"},
                            "first_name": {"type": "text"},
                            "last_name": {"type": "text"},
                            "email": {"type": "keyword"},
                            "age": {"type": "integer"}
                        }
                    }
                }
                """;
        URL url = new URL(this.url + "/" + index);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("PUT");
        conn.setDoOutput(true);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(indexDefinition.getBytes());
            os.flush();
        }

        int responseCode = conn.getResponseCode();
        System.out.println("Index creation response code: " + responseCode);
        conn.disconnect();
    }

    private void insertData() throws Exception {
        String[] employees = getEmployees();

        for (String employee : employees) {
            URL url = new URL(this.url + "/employees/_doc");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(employee.getBytes());
                os.flush();
            }

            int responseCode = conn.getResponseCode();
//            System.out.println("Insert data response code: " + responseCode);

            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String line;
                StringBuilder response = new StringBuilder();
                while ((line = br.readLine()) != null) {
                    response.append(line);
                }
//                System.out.println("Insert data response: " + response.toString());
            }

            conn.disconnect();
        }
    }

    private static String[] getEmployees() {
        return new String[]{
                """
                {
                  "id": 1,
                  "first_name": "John",
                  "last_name": "Doe",
                  "email": "john.doe@example.com",
                  "age": 30
                }
                """,
                """
                {
                  "id": 2,
                  "first_name": "Jane",
                  "last_name": "Smith",
                  "email": "jane.smith@example.com",
                  "age": 28
                }
                """,
                """
                {
                  "id": 3,
                  "first_name": "Alice",
                  "last_name": "Smith",
                  "email": "alice.smith@example.com",
                  "age": 30
                }
                """,
                """
                {
                  "id": 4,
                  "first_name": "Bob",
                  "last_name": "Johnson",
                  "email": "bob.johnson@example.com",
                  "age": 35
                }
                """,
                """
                {
                  "id": 5,
                  "first_name": "Charlie",
                  "last_name": "Williams",
                  "email": "charlie.williams@example.com",
                  "age": 28
                }
                """,
                """
                {
                  "id": 6,
                  "first_name": "David",
                  "last_name": "Brown",
                  "email": "david.brown@example.com",
                  "age": 42
                }
                """,
                """
                {
                  "id": 7,
                  "first_name": "Eva",
                  "last_name": "Jones",
                  "email": "eva.jones@example.com",
                  "age": 26
                }
                """,
                """
                {
                  "id": 8,
                  "first_name": "Frank",
                  "last_name": "Garcia",
                  "email": "frank.garcia@example.com",
                  "age": 33
                }
                """,
                """
                {
                  "id": 9,
                  "first_name": "Grace",
                  "last_name": "Martinez",
                  "email": "grace.martinez@example.com",
                  "age": 29
                }
                """,
                """
                {
                  "id": 10,
                  "first_name": "Hannah",
                  "last_name": "Anderson",
                  "email": "hannah.anderson@example.com",
                  "age": 31
                }
                """,
                """
                {
                  "id": 11,
                  "first_name": "Ivan",
                  "last_name": "Thomas",
                  "email": "ivan.thomas@example.com",
                  "age": 27
                }
                """,
                """
                {
                  "id": 12,
                  "first_name": "Jane",
                  "last_name": "Jackson",
                  "email": "jane.jackson@example.com",
                  "age": 36
                }
                """
        };
    }
}
