//including curl
#include "CurlEasyPtr.h"
#include <curl/curl.h>
//streams
#include <iostream>
#include <sstream>
#include <fstream>
//sockets
#include <sys/socket.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <arpa/inet.h>

#include <string.h>
#include <array>

#define MAX_ATTEMPTS 20
#define BUFFER_SIZE 1000

using namespace std::literals;

int connect_to_coord(char* host, char* port) {
   struct addrinfo hints;
   struct addrinfo* res;
   memset(&hints, 0, sizeof hints);
   hints.ai_family = AF_INET;
   hints.ai_socktype = SOCK_STREAM;;

   int sockfd;

   if (int status = getaddrinfo(host, port, &hints, &res); status != 0) {
      std::cerr << "Worker: getaddrinfo error: " << gai_strerror(status) << std::endl;
      freeaddrinfo(res);
      return -1;
   }

   sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
   if (sockfd == -1) {
      perror("Worker: socket error");
      freeaddrinfo(res);
      return -1;
   }

   int attempt = 0;
   while (attempt++ < MAX_ATTEMPTS && connect(sockfd, res->ai_addr, res->ai_addrlen) == -1) {
      close(sockfd);
      perror("Worker: connect error");
   }

   freeaddrinfo(res);
   return sockfd;
}


int send_msg(int sockfd, std::string msg) {
   if (send(sockfd, msg.c_str(), msg.length(), 0) == -1) {
      perror("Worker: sent failed");
      return -1;
   }

   return 0;
}


int solve_task(std::string task) {
   auto curl = CurlEasyPtr::easyInit();

   int result = 0;
   // Iterate over all files
   curl.setUrl(task);
   // Download them
   auto csvData = curl.performToStringStream();
   for (std::string row; std::getline(csvData, row, '\n');) {
      auto rowStream = std::stringstream(std::move(row));

      // Check the URL in the second column
      unsigned columnIndex = 0;
      for (std::string column; std::getline(rowStream, column, '\t'); ++columnIndex) {
         // column 0 is id, 1 is URL
         if (columnIndex == 1) {
            // Check if URL is "google.ru"
            auto pos = column.find("://"sv);
            if (pos != std::string::npos) {
               auto afterProtocol = std::string_view(column).substr(pos + 3);
               if (afterProtocol.starts_with("google.ru/"))
                  ++result;
            }
            break;
         }
      }
   }

   return result;
}


/// Worker process that receives a list of URLs and reports the result
int main(int argc, char* argv[]) {
   if (argc != 3) {
      std::cerr << "Usage: " << argv[0] << " <host> <port>" << std::endl;
      return 1;
   }

   std::cout << "Worker started with host: " << argv[1] << std::endl;

   auto curlSetup = CurlGlobalSetup();
   int sockfd = connect_to_coord(argv[1], argv[2]);
   if (sockfd == -1) {
      return -1;
   }

   while (true) {
      std::array<char, BUFFER_SIZE> buffer{};
      int nbytes = (int)recv(sockfd, &buffer, buffer.size(), 0);
      //error or socket close
      if (nbytes <= 0)
         break;

      //received valid data
      std::string task(buffer.data(), nbytes);

      if (send_msg(sockfd, std::to_string(solve_task(task))) == -1) {
         std::cerr << "Worker: error sending msg" << std::endl;
         close(sockfd);
         return 1;
      }
   }

   close(sockfd);
   return 0;
}
