#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <queue>
#include <mutex>
#include <thread>
#include <atomic>
#include <map>
#include <curl/curl.h>
#include <mosquittopp.h>

// konfiguracja InfluxDB
const std::string INFLUX_TOKEN  = "INFLUXDB_API_TOKEN"; 
const std::string INFLUX_ORG    = "Projekt_C4";
const std::string INFLUX_BUCKET = "PSCR";
const std::string INFLUX_URL    = "http://localhost:8086/api/v2/write?org=" + INFLUX_ORG + "&bucket=" + INFLUX_BUCKET + "&precision=ms";


std::queue<std::string> influxQueue;
std::mutex queueMutex;

// Funkcja pomocnicza do dzielenia stringow
std::vector<std::string> split(const std::string& s, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(s);
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

// stacja C3 - dane energetyczne
class C3Task : public mosqpp::mosquittopp {
    std::atomic<long long> lastTimestamp{0};

public:
    C3Task(const char *id) : mosquittopp(id) {}

    void on_connect(int rc) override {
        if (rc == 0) {
            subscribe(NULL, "C3_data/#");
        }
    }

    void on_message(const struct mosquitto_message *msg) override {
        std::string topic = msg->topic;
        std::string payload = (char*)msg->payload;

        // Obsługa znacznika czasu
        if (topic == "C3_data/timestamp") {
            try {
                lastTimestamp = std::stoll(payload);
                std::cout << "[MQTT C3] Ustawiono timestamp: " << lastTimestamp << std::endl;
            } catch (...) {}
            return;
        }

        if (lastTimestamp == 0) return;

        auto parts = split(topic, '/');
        if (parts.size() < 2) return;

        std::string tags = "stacja=C3";
        std::string field = (parts.size() == 3) ? (tags += ",source=" + parts[1], parts[2]) : parts[1];
        
        std::string line = "energetyka," + tags + " " + field + "=" + payload + " " + std::to_string(lastTimestamp);
        
        std::lock_guard<std::mutex> lock(queueMutex);
        influxQueue.push(line);
    }
};

// stacja C2 - dane pogodowe
class C2Task : public mosqpp::mosquittopp {
    std::map<std::string, long long> locTimestamps;
    std::mutex mapMutex;

public:
    C2Task(const char *id) : mosquittopp(id) {}

    void on_connect(int rc) override { 
        if (rc == 0) subscribe(NULL, "c2_c4/#"); 
    }

    void on_message(const struct mosquitto_message *msg) override {
        std::string topic = msg->topic;
        std::string payload = (char*)msg->payload;
        auto parts = split(topic, '/');
        
        if (parts.size() < 3) return;
        
        std::string loc = parts[1];  // lokalizacja lub avg temperatura
        std::string type = parts[2];

        // Obsługa znacznika czasu  s -> ms
        if (type == "time" || type == "timestamp") {
            try {
                double seconds = std::stod(payload);
                long long ms = static_cast<long long>(seconds * 1000.0);

                std::lock_guard<std::mutex> lock(mapMutex);
                locTimestamps[loc] = ms;
                std::cout << "[MQTT C2] Nowy czas dla [" << loc << "]: " << ms << " ms" << std::endl;
            } catch (...) {
                std::cerr << "[MQTT C2] Blad konwersji czasu dla " << loc << ": " << payload << std::endl;
            }
            return;
        }

        long long currentTs = 0;
        {
            std::lock_guard<std::mutex> lock(mapMutex);
            if (locTimestamps.count(loc)) currentTs = locTimestamps[loc];
        }

        if (currentTs > 0) {
            std::string line = "pogoda,stacja=C2,lokalizacja=" + loc + " " + type + "=" + payload + " " + std::to_string(currentTs);
            
            std::lock_guard<std::mutex> lock(queueMutex);
            influxQueue.push(line);
            std::cout << "[MQTT C2] Zakolejkowano dane dla [" << loc << "]: " << type << "=" << payload << std::endl;
        }
    }
};

// wysyłanie danych do InfluxDB
void influxWriterTask() {
    CURL *curl = curl_easy_init();
    if (!curl) return;

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, ("Authorization: Token " + INFLUX_TOKEN).c_str());
    headers = curl_slist_append(headers, "Content-Type: text/plain; charset=utf-8");

    while (true) {
        std::string data = "";

        {
            std::lock_guard<std::mutex> lock(queueMutex);
            if (!influxQueue.empty()) {
                data = influxQueue.front();
                influxQueue.pop();
            }
        }

        if (!data.empty()) {
            curl_easy_setopt(curl, CURLOPT_URL, INFLUX_URL.c_str());
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data.c_str());
            
            CURLcode res = curl_easy_perform(curl);
            if (res != CURLE_OK) {
                std::cerr << "[InfluxDB] BLAD: " << curl_easy_strerror(res) << std::endl;
            } else {
                std::cout << "[InfluxDB] Zapisano pomyslnie" << std::endl;
            }
        }
        

        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}

int main() {
    // inicjalizacja bibliotek
    mosqpp::lib_init();
    curl_global_init(CURL_GLOBAL_ALL);

    C2Task c2_sub("C4_Reader_C2");
    C3Task c3_sub("C4_Reader_C3");

    const char* mqtt_host = "BROKER_IP_ADDRESS";

    // Łączenie z brokerem MQTT
    if (c2_sub.connect(mqtt_host, 1883, 60) != MOSQ_ERR_SUCCESS) {
        std::cerr << "[CRITICAL] Nie mozna polaczyc z brokerem dla C2" << std::endl;
    }
    if (c3_sub.connect(mqtt_host, 1883, 60) != MOSQ_ERR_SUCCESS) {
        std::cerr << "[CRITICAL] Nie mozna polaczyc z brokerem dla C3" << std::endl;
    }

    std::cout << "[SYSTEM] Startowanie watkow roboczych..." << std::endl;

    // Uruchomienie wątków
    std::thread t2([&](){ c2_sub.loop_forever(); });
    std::thread t3([&](){ c3_sub.loop_forever(); });
    std::thread tw(influxWriterTask);

    t2.join(); 
    t3.join(); 
    tw.join();

    return 0;
}