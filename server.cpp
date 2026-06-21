#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>
#include <iostream>
#include <memory>
#include <vector>
#include <deque>
#include <queue>

// Условный заголовок для Protobuf сообщений
// #include "game_messages.pb.h"

using boost::asio::ip::tcp;

// Класс, представляющий одного подключенного клиента
class GameClient : public std::enable_shared_from_this<GameClient>
{
public:
    GameClient(tcp::socket socket) : socket_(std::move(socket)) {}

    tcp::socket &socket() { return socket_; }
    void set_name(const std::string &name) { name_ = name; }
    std::string get_name() const { return name_; }

    // Метод для отправки сообщения клиенту
    void send_message(const std::string &message)
    {
        // В реальном проекте здесь была бы асинхронная запись с очередью
        std::cout << "[Server] Sending to " << name_ << ": " << message << std::endl;
        boost::asio::async_write(socket_, boost::asio::buffer(message + "\n"), [](const boost::system::error_code &error, size_t)
                                 {
                if (error) {
                    std::cerr << "Send error: " << error.message() << std::endl;
                } });
        // ... код для записи в сокет ...
    }

private:
    tcp::socket socket_;
    std::string name_ = "Anon";
};

class Command
{
private:
public:
    std::string command;
    std::shared_ptr<GameClient> client;
};

// Класс MMO Сервера
class MmoServer
{
public:
    MmoServer(boost::asio::io_context &io_context, short port)
        : io_context_(io_context),
          acceptor_(io_context, tcp::endpoint(tcp::v4(), port))
    {
        std::cout << "MMO Server started on port " << port << std::endl;
        start_accept();
        // Запускаем игровой цикл
        boost::asio::post(io_context_, boost::bind(&MmoServer::game_loop, this));
    }

private:
    std::queue<Command> taskQueue;

    void start_accept()
    {
        auto new_client = std::make_shared<GameClient>(tcp::socket(io_context_));
        acceptor_.async_accept(new_client->socket(),
                               boost::bind(&MmoServer::handle_accept, this, new_client, boost::asio::placeholders::error));
    }

    void handle_accept(std::shared_ptr<GameClient> client, const boost::system::error_code &error)
    {
        if (!error)
        {
            clients_.push_back(client);
            std::cout << "[+] Client connected. Total: " << clients_.size() << std::endl;
            // Начинаем чтение от клиента
            start_read(client);
        }
        else
        {
            std::cerr << "Accept error: " << error.message() << std::endl;
        }
        start_accept(); // Принимаем следующего
    }

    void start_read(std::shared_ptr<GameClient> client)
    {
        auto buffer = std::make_shared<boost::asio::streambuf>();

        // Читаем до символа новой строки
        boost::asio::async_read_until(client->socket(), *buffer, '\n',
                                      [this, client, buffer](const boost::system::error_code &error, size_t length)
                                      {
                                          if (!error)
                                          {
                                              std::istream is(buffer.get());
                                              std::string message;
                                              std::getline(is, message);

                                              // Обрабатываем команду
                                              // process_command(client, message);
                                              taskQueue.push({message, client});
                                              // Продолжаем читать
                                              start_read(client);
                                          }
                                          else
                                          {
                                              // Клиент отключился
                                              std::cout << "[-] Client " << client->get_name() << " disconnected" << std::endl;
                                              auto it = std::find(clients_.begin(), clients_.end(), client);
                                              if (it != clients_.end())
                                              {
                                                  clients_.erase(it);
                                              }
                                          }
                                      });

        // В реальном проекте здесь буфер и асинхронное чтение с обработкой заголовков
        // ...
    }

    void process_command(std::shared_ptr<GameClient> client, const std::string &command)
    {
        std::cout << "Received from " << client->get_name() << ": " << command << std::endl;

        // Обработка команды "/name"
        if (command.rfind("/name ", 0) == 0)
        {
            std::string new_name = command.substr(6);
            client->set_name(new_name);
            client->send_message("Name changed to " + new_name);
        }
        else
        {
            // Широковещательная рассылка (чат)
            broadcast(client->get_name() + ": " + command);
        }
    }

    void broadcast(const std::string &message)
    {
        for (auto &client : clients_)
        {
            client->send_message(message);
        }
    }

    void game_loop()
    {
        // 1. Обрабатываем очередь команд (интеншены игроков)
        // 2. Обновляем состояние мира (AI, физика, регенерация)
        // 3. Рассылаем обновления игрокам в зоне видимости
        /*if (clients_.size() > 0)
            clients_.at(0)->send_message("Hi! " + std::to_string(
                                                      std::chrono::duration_cast<std::chrono::seconds>(
                                                          std::chrono::system_clock::now().time_since_epoch())
                                                          .count()));*/
        std::cout << "[Tick] World updated. Clients: " << clients_.size() << std::endl;
        if (!taskQueue.empty())
        {
            auto command = taskQueue.front();
            process_command(command.client, command.command);
            taskQueue.pop();
        }
        // Планируем следующий тик через 100 мс
        auto timer = std::make_shared<boost::asio::steady_timer>(io_context_, std::chrono::milliseconds(1000));
        timer->async_wait([this, timer](const boost::system::error_code &)
                          { game_loop(); });
    }

    boost::asio::io_context &io_context_;
    tcp::acceptor acceptor_;
    std::vector<std::shared_ptr<GameClient>> clients_;
};

int main()
{
    try
    {
        boost::asio::io_context io_context;
        MmoServer server(io_context, 8888);
        io_context.run(); // Запускаем цикл обработки событий Asio
    }
    catch (std::exception &e)
    {
        std::cerr << "Exception: " << e.what() << std::endl;
    }
    return 0;
}