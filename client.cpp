#include <boost/asio.hpp>
#include <iostream>
#include <thread>

using boost::asio::ip::tcp;

class MmoClient {
public:
    MmoClient(boost::asio::io_context& io_context, const std::string& host, short port)
        : io_context_(io_context), socket_(io_context) {
        tcp::resolver resolver(io_context);
        auto endpoints = resolver.resolve(host, std::to_string(port));
        boost::asio::async_connect(socket_, endpoints,
            [this](const boost::system::error_code& error, const tcp::endpoint&) {
                if (!error) {
                    std::cout << "Connected to server." << std::endl;
                    // Запускаем слушатель сервера
                    start_listen();
                } else {
                    std::cerr << "Connect error: " << error.message() << std::endl;
                }
            });
    }

    void send_command(const std::string& command) {
        // Асинхронная отправка данных на сервер
        boost::asio::async_write(socket_, boost::asio::buffer(command + "\n"),
            [](const boost::system::error_code& error, size_t) {
                if (error) {
                    std::cerr << "Send error: " << error.message() << std::endl;
                }
            });
    }

private:
    void start_listen() {
        // Выделяем буфер для чтения
        auto buffer = std::make_shared<boost::asio::streambuf>();
        boost::asio::async_read_until(socket_, *buffer, '\n',
            [this, buffer](const boost::system::error_code& error, size_t length) {
               // std::cout << "QWE" << std::endl;
                if (!error) {
                    std::istream is(buffer.get());
                    std::string server_message;
                    std::getline(is, server_message);
                    // Обрабатываем входящее обновление мира
                    process_server_update(server_message);
                    start_listen(); // Слушаем дальше
                } else {
                    std::cerr << "Read error: " << error.message() << std::endl;
                    socket_.close();
                }
            });
    }

    void process_server_update(const std::string& message) {
        // В реальном проекте здесь парсинг Protobuf сообщений
        std::cout << "[Server Update] " << message << std::endl;
    }

    boost::asio::io_context& io_context_;
    tcp::socket socket_;
};

int main() {
    boost::asio::io_context io_context;
    MmoClient client(io_context, "127.0.0.1", 8888);

    // Запускаем поток для обработки ввода пользователя
    std::thread input_thread([&client]() {
        std::string input;
        while (std::getline(std::cin, input)) {
            if (input == "quit") break;
            client.send_command(input);
        }
    });

    io_context.run(); // Обрабатываем асинхронные события
    input_thread.join();
    return 0;
}