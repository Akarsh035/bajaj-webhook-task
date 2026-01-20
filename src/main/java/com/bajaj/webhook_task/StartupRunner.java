package com.bajaj.webhook_task;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;
import com.bajaj.webhook_task.model.webhookResponse;

import java.util.Map;
import java.util.HashMap;

@Component
public class StartupRunner implements CommandLineRunner {
    private final RestTemplate restTemplate;

    public StartupRunner(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }
    @Override
    public void run(String... args) {
        String url = "https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA";
        Map<String, String> body = new HashMap<>();
        body.put("name", "Akarsh");
        body.put("regNo", "22BPS1182");
        body.put("email", "akarsh035@gmail.com");
        HttpEntity<Map<String, String>> request =
                new HttpEntity<>(body);
        ResponseEntity<webhookResponse> response =
                restTemplate.postForEntity(
                        url,
                        request,
                        webhookResponse.class
                );
        webhookResponse resp = response.getBody();

        if (resp == null) {
            throw new RuntimeException("Response body is null");
        }

        String webhookUrl = resp.getWebhook();
        String accessToken = resp.getAccessToken();

        System.out.println("Webhook URL = " + webhookUrl);
        System.out.println("Access Token = " + accessToken);

        String finalQuery="SELECT\n" +
                "    d.DEPARTMENT_NAME,\n" +
                "    AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, e.DOB))) AS AVERAGE_AGE,\n" +
                "    STRING_AGG(e.FIRST_NAME || ' ' || e.LAST_NAME, ', ' ORDER BY e.FIRST_NAME)\n" +
                "        FILTER (WHERE rn <= 10) AS EMPLOYEE_LIST\n" +
                "FROM (\n" +
                "    SELECT\n" +
                "        e.*,\n" +
                "        d.DEPARTMENT_NAME,\n" +
                "        ROW_NUMBER() OVER (\n" +
                "            PARTITION BY d.DEPARTMENT_ID\n" +
                "            ORDER BY e.FIRST_NAME\n" +
                "        ) AS rn\n" +
                "    FROM EMPLOYEE e\n" +
                "    JOIN DEPARTMENT d\n" +
                "        ON e.DEPARTMENT = d.DEPARTMENT_ID\n" +
                "    JOIN PAYMENTS p\n" +
                "        ON p.EMP_ID = e.EMP_ID\n" +
                "    WHERE p.AMOUNT > 70000\n" +
                ") e\n" +
                "JOIN DEPARTMENT d\n" +
                "    ON e.DEPARTMENT = d.DEPARTMENT_ID\n" +
                "GROUP BY d.DEPARTMENT_ID, d.DEPARTMENT_NAME\n" +
                "ORDER BY d.DEPARTMENT_ID DESC;\n";
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", accessToken);
        headers.setContentType(MediaType.APPLICATION_JSON);
        Map<String, String> payload = new HashMap<>();
        payload.put("finalQuery", finalQuery);
        HttpEntity<Map<String, String>> entity =
                new HttpEntity<>(payload, headers);
        restTemplate.postForEntity(
                webhookUrl,
                entity,
                String.class
        );
        System.out.println("Final SQL submitted successfully");


    }
}

