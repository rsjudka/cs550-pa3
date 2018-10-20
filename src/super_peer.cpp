#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netdb.h>

#include <thread>
#include <mutex>
#include <unordered_map>
#include <iostream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <chrono>
#include <fstream>


#define HOST "localhost" // assume all connections happen on same machine
#define MAX_FILENAME_SIZE 256 // assume the maximum file size is 256 characters
#define MAX_MSG_SIZE 4096


class SuperPeer {
    private:
        std::vector<int> _peers;
        std::vector<int> _nodes;

        std::unordered_map<std::string, std::vector<int>> _files_index; // mapping between a filename and any peers associated with it
        
        struct _message_id_hash {
            size_t operator()(const std::pair<int, int>& p) const { return p.first ^ p.second; }
        };
        
        typedef std::pair<int, int> _message_id_t;
        std::unordered_map<_message_id_t, std::chrono::system_clock::time_point, _message_id_hash> _message_ids;
        
        std::ofstream _server_logs;

        std::mutex _files_index_m;
        std::mutex _message_ids_m;
        std::mutex _log_m;

        // helper function for getting the current time to microsecond-accuracy as a string
        std::string time_now() {
            std::chrono::high_resolution_clock::duration now = std::chrono::high_resolution_clock::now().time_since_epoch();
            std::chrono::microseconds now_ms = std::chrono::duration_cast<std::chrono::microseconds>(now);
            return std::to_string(now_ms.count());
        }

        // log message to specified log file
        void log(std::string type, std::string msg) {
            std::lock_guard<std::mutex> guard(_log_m);
            _server_logs << '[' << time_now() << "] [" << type << "] " << msg  << '\n' << std::endl;
            std::cout << '[' << time_now() << "] [" << type << "] [" << msg  << "]\n" << std::endl;
        }
        
        void error(std::string type) {
            std::cerr << "\n[" << type << "] exiting program\n" << std::endl;
            exit(1);
        }

        //helper function for cleaning up the indexing server anytime a node is disconnected
        void remove_node(int socket_fd, int id, std::string type) {
            std::string msg = "closing connection for id '" + std::to_string(id) + "' and cleaning up index";
            log(type, msg);
            files_index_cleanup(id);
            close(socket_fd);
        }

        // create a connection to some server given a specific port
        int connect_server(int port) {
            struct sockaddr_in addr;
            socklen_t addr_size = sizeof(addr);
            bzero((char *)&addr, addr_size);

            // open a socket for the new connection
            struct hostent *server = gethostbyname(HOST);
            int socket_fd = socket(AF_INET, SOCK_STREAM, 0);

            addr.sin_family = AF_INET;
            bcopy((char *)server->h_addr, (char *)&addr.sin_addr.s_addr, server->h_length);
            addr.sin_port = htons(port);
            
            // connect to the server
            if (connect(socket_fd, (struct sockaddr *)&addr, addr_size) < 0) {
                return -1;
            }
            
            return socket_fd;
        }

        // handle all requests sent to the peer
        void handle_connection(int socket_fd) {
            char id;
            //initialize connection by getting id
            if (recv(socket_fd, &id, sizeof(id), 0) < 0) {
                log("conn unidentified", "closing connection");
                close(socket_fd);
                return;
            }

            switch (id) {
                case '0':
                    handle_peer_request(socket_fd);
                    break;
                case '1':
                    handle_node_request(socket_fd);
                    break;
                default:
                    log("conn unidentified", "closing connection");
                    close(socket_fd);
                    return;
            }
        }

        // handle request from neighbor peers
        void handle_peer_request(int socket_fd) {
            // get the ttl value of the sent message
            int ttl;
            if (recv(socket_fd, &ttl, sizeof(ttl), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                return;
            }

            // get the id part of the message id
            int id;
            if (recv(socket_fd, &id, sizeof(id), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                return;
            }

            // get the sequence message part of the message id
            int sequence_number;
            if (recv(socket_fd, &sequence_number, sizeof(sequence_number), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                return;
            }            

            char buffer[MAX_FILENAME_SIZE];
            // recieve filename
            if (recv(socket_fd, buffer, sizeof(buffer), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                return;
            }

            std::string ids;
            // check if message id has been seen/forwarded already
            if (check_message_id(socket_fd)) {
                // get all ids from local files index
                ids = query_local_files_index(buffer);
                if (ttl-- > 0) {
                    // get all nodes ids from all neighbor peers' files indexes
                    std::string peers_ids = query_peers_files_index(buffer, id, sequence_number, ttl);
                    // only add anything if ids where found in peers
                    if (!peers_ids.empty())
                        ids += ((!ids.empty()) ? "," : "") + peers_ids;
                }
            }

            char buffer_[MAX_MSG_SIZE];
            strcpy(buffer_, ids.c_str());
            // send comma delimited list of all ids for a specific file to the peer
            if (send(socket_fd, buffer_, sizeof(buffer_), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                return;
            }
            close(socket_fd);
            log("peer disconnected", "closed connection");
        }

        // handle all requests sent to the indexing server
        void handle_node_request(int socket_fd) {
            int id;
            //initialize connection with node by getting id
            if (recv(socket_fd, &id, sizeof(id), 0) < 0) {
                log("node unidentified", "closing connection");
                close(socket_fd);
                return;
            }

            char request;
            while (1) {
                request = '0';
                // get request type from node
                if (recv(socket_fd, &request, sizeof(request), 0) < 0) {
                    remove_node(socket_fd, id, "node unresponsive");
                    return;
                }

                switch (request) {
                    case '1':
                        registry(socket_fd, id);
                        break;
                    case '2':
                        deregistry(socket_fd, id);
                        break;
                    case '3':
                        node_search(socket_fd, id);
                        break;
                    case '4':
                        print_files_map();
                        break;
                    case '5':
                        print_message_ids_list();
                        break;
                    case '0':
                        remove_node(socket_fd, id, "node disconnected");
                        return;
                    default:
                        remove_node(socket_fd, id, "unexpected request");
                        return;
                }
            }
        }

        // handles communication with node for registering a single file 
        void registry(int socket_fd, int id) {
            char buffer[MAX_FILENAME_SIZE];
            // recieve filename from node
            if (recv(socket_fd, buffer, sizeof(buffer), 0) < 0) {
                remove_node(socket_fd, id, "node unresponsive");
                return;
            }
            
            std::string filename = std::string(buffer);
            std::lock_guard<std::mutex> guard(_files_index_m);
            // add peer's id to file map if not already included
            if(!(std::find(_files_index[filename].begin(), _files_index[filename].end(), id) != _files_index[filename].end()))
                _files_index[filename].push_back(id);
        }

        // handles communication with node for deregistering a single file 
        void deregistry(int socket_fd, int id) {
            char buffer[MAX_FILENAME_SIZE];
            if (recv(socket_fd, buffer, sizeof(buffer), 0) < 0) {
                remove_node(socket_fd, id, "node unresponsive");
                return;
            }
 
            std::string filename = std::string(buffer);
            std::lock_guard<std::mutex> guard(_files_index_m);
            // remove peer's id from file
            _files_index[filename].erase(std::remove(_files_index[filename].begin(),
                                            _files_index[filename].end(), id), _files_index[filename].end());
            // remove filename from mapping if no more peers mapped to file
            if (_files_index[filename].size() == 0)
                _files_index.erase(filename);
        }

        // remove id from all files in mapping
        void files_index_cleanup(int id) {
            std::unordered_map<std::string, std::vector<int>> tmp_files_index;
            
            std::lock_guard<std::mutex> guard(_files_index_m);
            for (auto const &file_index : _files_index) {
                std::vector<int> ids = file_index.second;
                ids.erase(std::remove(ids.begin(), ids.end(), id), ids.end());
                if (ids.size() > 0)
                    tmp_files_index[file_index.first] = ids;
            }
            _files_index = tmp_files_index;
        }

        // searches local files index for filename
        std::string query_local_files_index(std::string filename) {
            std::ostringstream ids;
            if (_files_index.count(filename) > 0) {
                std::string delimiter;
                std::lock_guard<std::mutex> guard(_files_index_m);
                for (auto &&id : _files_index[filename]) {
                    // add id to stream
                    ids << delimiter << id;
                    delimiter = ',';
                }
            }
            return ids.str();
        }

        // searches all peers' files indexes for filename
        std::string query_peers_files_index(std::string filename, int id, int sequence_number, int ttl) {
            std::string ids;
            std::string delimiter;
            // iteratively query each peer to search their files index
            for (auto&& peer : _peers) {
                int socket_fd = connect_server(peer);
                if (socket_fd < 0) {
                    log("failed peer connection", "ignoring connection");
                    continue;
                }
                if (send(socket_fd, "0", sizeof(char), 0) < 0)
                    log("peer unresponsive", "ignoring request");
                else {
                    // send ttl value of current message
                    if (send(socket_fd, &ttl, sizeof(ttl), 0) < 0)
                        log("peer unresponsive", "ignoring request");
                    else {
                        // send the id part of the message id
                        if (send(socket_fd, &id, sizeof(id), 0) < 0)
                            log("peer unresponsive", "ignoring request");
                        else {
                            // send the sequence number part of the message id
                            if (send(socket_fd, &sequence_number, sizeof(sequence_number), 0) < 0)
                                log("peer unresponsive", "ignoring request");
                            else {
                                char buffer[MAX_FILENAME_SIZE];
                                strcpy(buffer, filename.c_str());
                                // send the filename of the request
                                if (send(socket_fd, buffer, sizeof(buffer), 0) < 0)
                                    log("peer unresponsive", "ignoring request");
                                else {
                                    if (!send_message_id(socket_fd, id, sequence_number))
                                        log("peer unresponsive", "ignoring request");
                                    else {
                                        std::string msg = "msg id [" + std::to_string(id) + "," +
                                                std::to_string(sequence_number) + "] to peer " + std::to_string(peer);
                                        log("forwarding message", msg);
                                        char buffer_[MAX_MSG_SIZE];
                                        // get the list of ids from the peer
                                        if (recv(socket_fd, buffer_, sizeof(buffer_), 0) < 0)
                                            log("peer unresponsive", "ignoring request");
                                        else if (buffer_[0]) {
                                            // add the list of ids to our 'global' list of all ids for this query
                                            ids += delimiter + std::string(buffer_);
                                            delimiter = ',';
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                close(socket_fd);
            }
            return ids;
        }

        // helper function for sending the parts of a message id
        bool send_message_id(int socket_fd, int id, int sequence_number) {
            if (send(socket_fd, &id, sizeof(id), 0) < 0)
                return false;

            if (send(socket_fd, &sequence_number, sizeof(sequence_number), 0) < 0)
                return false;
            
            std::lock_guard<std::mutex> guard(_message_ids_m);
            // add the message id and the time it was requested to the global message ids list
            _message_ids[{id, sequence_number}] = std::chrono::system_clock::now();
            return true;
        }

        // checks if message was already seen/forwarded
        bool check_message_id(int socket_fd) {
            int id;
            // get id part of message id
            if (recv(socket_fd, &id, sizeof(id), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                return false;
            }

            int sequence_number;
            // get sequence number of message id
            if (recv(socket_fd, &sequence_number, sizeof(sequence_number), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                return false;
            }

            _message_id_t msg_id = {id, sequence_number};
            std::lock_guard<std::mutex> guard(_message_ids_m);
            if (_message_ids.find(msg_id) == _message_ids.end()) {
                // add message id to global list if not found
                _message_ids[msg_id] = std::chrono::system_clock::now();
                return true;
            }
            log("message already seen", "rerouting message back to sender");
            return false;
        }
        
        // handles communication with node for returning all ids mapped to a filename
        void node_search(int socket_fd, int id) {
            char buffer[MAX_FILENAME_SIZE];
            // recieve filename from node
            if (recv(socket_fd, buffer, sizeof(buffer), 0) < 0) {
                remove_node(socket_fd, id, "node unresponsive");
                return;
            }

            _sequence_number += 1;
            // get ids from local files index
            std::string ids = query_local_files_index(buffer);
            // get all nodes ids from all neighbor peers' files indexes
            std::string peers_ids = query_peers_files_index(buffer, id, _sequence_number, _ttl);
            if (!peers_ids.empty())
                ids += ((!ids.empty()) ? "," : "") + peers_ids;
            
            char buffer_[MAX_MSG_SIZE];
            strcpy(buffer_, ids.c_str());
            // send comma delimited list of all ids for a specific file to the node
            if (send(socket_fd, buffer_, sizeof(buffer_), 0) < 0) {
                remove_node(socket_fd, id, "node unresponsive");
                return;
            }
        }

        // helper function for displaying the entire files index
        void print_files_map() {
            std::lock_guard<std::mutex> guard(_files_index_m);
            std::cout << "\n__________FILES INDEX__________" << std::endl;
            for (auto const &file_index : _files_index) {
                std::cout << file_index.first << ':';
                std::string delimiter;
                for (auto &&id : file_index.second) {
                    std::cout << delimiter << id;
                    delimiter = ',';
                }
                std::cout << std::endl;
            }
            std::cout << "_______________________________\n" << std::endl;
        }

        // helper function for displaying all message ids currently being tracked
        void print_message_ids_list() {
            std::lock_guard<std::mutex> guard(_files_index_m);
            std::cout << "\n__________MESSAGE IDS__________" << std::endl;
            for (auto const &message_id : _message_ids) {
                std::cout << '[' << message_id.first.first << ',' << message_id.first.second << "]" << std::endl;
            }
            std::cout << "_______________________________\n" << std::endl;
        }

        // gets the static network info for a peer
        void get_network(std::string config_path) {
            // open a stream to the config file
            std::ifstream config(config_path);
            int member_type;
            int id;
            int port;
            std::string peers;
            std::string nodes;

            config >> _ttl;
            std::string tmp;
            // proper config syntax is expected to be followed
            while(config >> member_type) {
                if (member_type == 0) {
                    config >> id >> port >> peers >> nodes;
                    if (id == _id) {
                        _port = port;
                        _peers = comma_delim_ints_to_vector(peers);
                        _nodes = comma_delim_ints_to_vector(nodes);
                        return;
                    }
                }
                else
                    std::getline(config, tmp); // ignore anything else in the line
            }
            error("invalid id");
        }

        // thread which continously runs, checking for old messages and removing them
        void maintain_message_ids() {
            while (1) {
                // wait 1 minute to check messages list
                sleep(60);
                std::lock_guard<std::mutex> guard(_message_ids_m);
                for (auto itr = _message_ids.cbegin(); itr != _message_ids.cend();) {
                    // remove messages if they were recorded longer than a minute ago
                    itr = (std::chrono::duration_cast<std::chrono::minutes>(std::chrono::system_clock::now() -
                                                        itr->second).count() > 1) ? _message_ids.erase(itr++) : ++itr;
                }
            }
        }

    public:
        int _ttl;
        int _id;
        int _port;
        int _socket_fd;
        int _sequence_number = 0;

        SuperPeer(int id, std::string config_path) {
            _id = id;
            get_network(config_path);

            struct sockaddr_in addr;
            socklen_t addr_size = sizeof(addr);
            bzero((char*)&addr, addr_size);
            
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port = htons(_port);

            _socket_fd = socket(AF_INET, SOCK_STREAM, 0);

            // bind socket to port to be used for indexing server
            if (bind(_socket_fd, (struct sockaddr*)&addr, addr_size) < 0)
                error("failed server binding");

            std::cout << "starting indexing server on port " << _port << '\n' << std::endl;

            // start logging
            _server_logs.open("logs/super_peers/" + std::to_string(_port) + ".log");
        }

        std::vector<int> comma_delim_ints_to_vector(std::string s) {
            std::vector<int> result;

            std::stringstream ss(s);
            while(ss.good()) {
                std::string substr;
                std::getline(ss, substr, ',');
                result.push_back(atoi(substr.c_str()));
            }
            std::random_shuffle(result.begin(), result.end());
            return result;
        }

        void run() {
            struct sockaddr_in addr;
            socklen_t addr_size = sizeof(addr);
            int socket_fd;

            // start thread for maintaining messages list
            std::thread t(&SuperPeer::maintain_message_ids, this);
            t.detach();

            std::ostringstream connection;
            while (1) {
                // listen for any peer connections to start communication
                listen(_socket_fd, 5);

                if ((socket_fd = accept(_socket_fd, (struct sockaddr*)&addr, &addr_size)) < 0) {
                    // ignore any failed connections from nodes
                    log("failed connection", "ignoring connection");
                    continue;
                }

                connection << inet_ntoa(addr.sin_addr) << '@' << ntohs(addr.sin_port);
                log("conn established", connection.str());
                
                // start thread for single client-server communication
                std::thread t(&SuperPeer::handle_connection, this, socket_fd);
                t.detach(); // detaches thread and allows for next connection to be made without waiting

                connection.str("");
                connection.clear();
            }
        }

        ~SuperPeer() {
            close(_socket_fd);
            _server_logs.close();
        }
};


int main(int argc, char *argv[]) {
    // require peer id config path to be passed as arg
    if (argc < 3) {
        std::cerr << "usage: " << argv[0] << " id config_path" << std::endl;
        exit(0);
    }

    SuperPeer super_peer(atoi(argv[1]), argv[2]);
    super_peer.run();

    return 0;
}
