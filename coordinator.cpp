//including curl
#include "CurlEasyPtr.h"

//some streams
#include <iostream>
#include <sstream>

#include <string>
#include <string.h>

//sockets staff
#include <sys/socket.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <poll.h>

//structures
#include <unordered_map>
#include <array>
#include <queue>
#include <vector>

using namespace std::literals;

#define BUFFER_SIZE 1000
#define S_TIMEOUT 2500
#define LISTEN_BACKLOG 20


int get_listener_socket(char* port) {
   int sockfd;

   struct addrinfo* res;
   struct addrinfo hints;
   memset(&hints, 0, sizeof hints);
   hints.ai_family = AF_INET;
   hints.ai_socktype = SOCK_STREAM;;
   hints.ai_flags = AI_PASSIVE;

   if (int status = getaddrinfo(NULL, port, &hints, &res); status != 0) {
      std::cerr << "Coord: getaddrinfo error: " << gai_strerror(status) << std::endl;
      //freeAddrInfo -> camelCase
      //free_addr_info -> snake_case
      freeaddrinfo(res); // nocase
      return -1;
   }

   sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
   if (sockfd == -1) {
      perror("Coord: socket error");
      freeaddrinfo(res);
      return -1;
   }

   int yes = 1;
   setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));

   if (bind(sockfd, res->ai_addr, res->ai_addrlen) == -1) {
      perror("Coord: bind error");
      freeaddrinfo(res);
      return -1;
   }

   if (listen(sockfd, LISTEN_BACKLOG) == -1) {
      perror("Coord: listen error");
      freeaddrinfo(res);
      return -1;
   }

   freeaddrinfo(res);
   return sockfd;

}

std::queue<std::string> get_work(char* url) {
   auto tasks = std::queue<std::string>();

   auto listUrl = std::string(url);

   // Download the file list
   auto curl = CurlEasyPtr::easyInit();
   curl.setUrl(listUrl);
   auto fileList = curl.performToStringStream();

   // Iterate over all files
   for (std::string s; std::getline(fileList, s, '\n');) {
      tasks.push(s);
   }
   return tasks;
}

/// Leader process that coordinates workers. Workers connect on the specified port
/// and the coordinator distributes the work of the CSV file list.
int main(int argc, char* argv[]) {
   if (argc != 3) {
      std::cerr << "Usage: " << argv[0] << " <URL to csv list> <listen port>" << std::endl;
      return 1;
   }
   std::cout << "Coordinator started" << std::endl;

   //LISTENING 
   int lst_sockfd = get_listener_socket(argv[2]);
   if (lst_sockfd == -1) {
      std::cerr << "Coord: creating list socket failed" << std::endl;
      return 1;
   }

   //PREPARING WORK
   auto curlSetup = CurlGlobalSetup();
   std::queue<std::string> tasks = get_work(argv[1]);
   std::unordered_map<int, std::string> in_progress;
   int res = 0;

   //STARTING TO ACCEPT WORKERS
   auto pfds = std::vector<pollfd>();
   struct pollfd tmp;
   tmp.fd = lst_sockfd;
   tmp.events = POLLIN;
   pfds.push_back(tmp);

   auto send_task = [&](int fd) {
      auto task = tasks.front();

      if (send(fd, task.c_str(), task.length(), 0) == -1) {
         perror("Coordinator: sent failed");
         return -1;
      }
      else {
         tasks.pop();
         in_progress[fd] = task;
         return 0;
      }
      };

   auto remove_worker = [&](size_t index) -> void {
      in_progress.erase(pfds[index].fd);
      close(pfds[index].fd);
      pfds.erase(pfds.begin() + index);
      };

   auto add_worker = [&]() {
      std::cout << "New worker showed up" << std::endl;
      struct sockaddr_storage remoteaddr;
      socklen_t addrlen = sizeof(remoteaddr);
      int newfd = accept(lst_sockfd, (struct sockaddr*)&remoteaddr, &addrlen);

      if (newfd == -1) {
         perror("Coord: accept error");
         return -1;
      }

      if (tasks.empty()) {
         close(newfd);
         return 0;
      }

      if (send_task(newfd) == -1) {
         remove_worker(newfd);
         return -1;
      }

      struct pollfd tmp;
      tmp.fd = newfd;
      tmp.events = POLLIN;
      pfds.push_back(tmp);
      return 0;
      };

   //waiting for the first worker to show up (and connect(correctly)
   do {
      if (poll(pfds.data(), pfds.size(), -1) == -1) {
         perror("Coord: first poll failed");
         return 1;
      }
   } while (add_worker() == -1);

   int kkk = 0;
   //now we can start to work
   while (true) {
      //wait for something to happen
      if (poll(pfds.data(), pfds.size(), -1) == -1) {
         perror("Coord:poll failed");
         return 1;
      }

      for (size_t i = 0; i < pfds.size(); i++) {
         //check for socket responsible for the event
         if (pfds[i].revents & POLLIN) {
            if (pfds[i].fd == lst_sockfd) {
               //we have a new worker
               if (add_worker() == -1) {
                  std::cerr << "Add worker failed" << std::endl;
                  return 1;
               }
            }
            else {
               //we have received something
               std::array<char, BUFFER_SIZE> buffer{};
               int nbytes = (int)recv(pfds[i].fd, &buffer, buffer.size(), 0);
               if (nbytes <= 0) {
                  //error or socket close
                  if (in_progress.contains(pfds[i].fd))
                     tasks.push(in_progress[pfds[i].fd]);

                  remove_worker(i);
               }
               else {
                  //received valid data
                  std::cout << "Task completed " << kkk++ << std::endl;
                  std::string s(buffer.data(), nbytes);
                  res += atoi(s.c_str());

                  if (tasks.empty())
                     remove_worker(i);
                  else
                     send_task(pfds[i].fd);
               }
            }
         }
      }
      if (tasks.empty() && pfds.size() == 1) {
         break;
      }
   }

   close(lst_sockfd);
   std::cout << res << std::endl;
   return 0;
}
