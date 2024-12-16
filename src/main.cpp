#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <queue>
#include <thread>
#include <unordered_map> // Для использования хеш-таблицы
#include <vector>
#include <mutex> // Для работы с мьютексами
#include <condition_variable> // Для условных переменных
#include <filesystem> // Для работы с файловой системой

// Потокобезопасная очередь
template <typename T>
class BlockingQueue {
private:
    std::mutex mtx;
    std::condition_variable cv;
    std::queue<T> queue;
    bool finished = false; // Флаг завершения работы
public:
    void push(const T& item) {
        std::unique_lock<std::mutex> lock(mtx); // Захватываем мьютекс
        queue.push(std::move(item));
        cv.notify_one();  // Оповещаем один ожидающий поток-консьюмер
    }

    bool pop(T& item) {
        std::unique_lock<std::mutex> lock(mtx); // Захватываем мьютекс
        cv.wait(lock, [this] { return !queue.empty() || finished; });
        if (!queue.empty()) {
            item = std::move(queue.front());
            queue.pop();
            return true;
        }
        return false;
    }

    void setFinished() {
        std::unique_lock<std::mutex> lock(mtx); // Захватываем мьютекс
        finished = true;
        cv.notify_all(); // Уведомляем все потоки-консьюмеры
    }
};

struct Contact {
    std::string surname;
    std::string name;
    std::string patronymic;
    std::string phone;
};

// Producer: читает файл и помещает строки в очередь задач
void producer(const std::string& filename, BlockingQueue<std::pair<char, Contact>>& taskQueue) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Error! Failed to open file: " << filename << std::endl;
        return;
    }

    std::string line;
    while (std::getline(file, line)) {
        std::istringstream iss(line);
        Contact contact;
        if (std::getline(iss, contact.surname, ' ') &&
            std::getline(iss, contact.name, ' ') &&
            std::getline(iss, contact.patronymic, ' ') &&
            std::getline(iss, contact.phone, ' ')) {
            char key = contact.surname.empty() ? '#' : contact.surname[0];  // Первая буква фамилии
            taskQueue.push({key, contact});  // Передаем запись в очередь
            }
    }

    file.close();
    taskQueue.setFinished(); // Сигнал о завершении
}

// Consumer: сохраняет записи в файлы
void consumer(BlockingQueue<std::pair<char, Contact>>& taskQueue, std::unordered_map<char, std::ofstream>& outputFiles, const std::string& directory) {
    std::pair<char, Contact> task;

    while (taskQueue.pop(task)) {
        char key = task.first;
        const Contact& contact = task.second;

        // Формируем полный путь к файлу, добавляя имя файла к директории
        std::string filePath = directory + "/" + std::string(1, key) + ".txt";

        // Открываем файл для соответствующей буквы, если он ещё не открыт
        if (outputFiles.find(key) == outputFiles.end()) {
            outputFiles[key].open(filePath, std::ios::app);
            if (!outputFiles[key].is_open()) {
                std::cerr << "Error: Unable to open output file for key: " << key << std::endl;
                continue;
            }
        }
 
        // Записываем контакт в файл
    
        bool contactExists = false;
        if (std::filesystem::exists(filePath)) {
            std::ifstream inputFile(filePath);
            if (inputFile.is_open()) {
                std::string line;
                while (std::getline(inputFile, line)) {
                    std::istringstream iss(line);
                    Contact existingContact;
                    if (std::getline(iss, existingContact.surname, ' ') &&
                        std::getline(iss, existingContact.name, ' ') &&
                        std::getline(iss, existingContact.patronymic, ' ') &&
                        std::getline(iss, existingContact.phone, ' ')) {

                        if (existingContact.surname == contact.surname &&
                            existingContact.name == contact.name &&
                            existingContact.patronymic == contact.patronymic &&
                            existingContact.phone == contact.phone) {
                            contactExists = true;
                            break;
                        }
                    }
                }
                inputFile.close();
            }
        }

        if (!contactExists) {
            outputFiles[key] << contact.surname << " " << contact.name << " " << contact.patronymic << " " << contact.phone << "\n";
        }
    }
}

int main()
{
    const std::string filename = "contacts.txt";
    const std::string directory = "results";
    BlockingQueue<std::pair<char, Contact>> taskQueue;
    const int numConsumers = 4;

    std::vector<std::thread> consumersThreads;
    std::unordered_map<char, std::ofstream> outputFiles;

    if (!std::filesystem::exists(filename)) {
        std::cerr << "Error: File does not exist: " << filename << std::endl;
        return -1; // Завершение программы с ошибкой
    }

    // Создаем директорию, если она не существует
    if (!std::filesystem::exists(directory)) {
        std::filesystem::create_directory(directory);
    }

    for (int i = 0; i < numConsumers; ++i) {
        consumersThreads.emplace_back(consumer, std::ref(taskQueue), std::ref(outputFiles), directory);

    }

    std::thread producerThread(producer, filename, std::ref(taskQueue));

    // Дожидаемся завершения producer
    producerThread.join();

    // Дожидаемся завершения всех потребителей
    for (auto& thread: consumersThreads) {
        thread.join();
    }

    for(auto& file: outputFiles) {
        if (file.second.is_open()) {
            file.second.close();
        }
    }

    std::cout << "Processing complete. Check output files." << std::endl;
    return 0;
}
