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


enum CONSISTENCY_METHODS{PUSH, PULL_N, PULL_P}; // cleaner comparisons for consistency method in use


class SuperPeer {
    private:
        std::vector<int> _peers;
        std::vector<int> _nodes;

        std::unordered_map<std::string, std::vector<int>> _files_index; // mapping between a filename and any peers associated with it

        struct _file {
            std::string name; // name of file modified
            int id; // origin node fo modified file
            time_t version; // version of the modified file
        };
        std::vector<_file> _modified_files; // vector of files modified from leaf nodes to check every ttr
        
        struct _message_id_hash {
            size_t operator()(const std::pair<int, int>& p) const { return p.first ^ p.second; }
        };
        
        typedef std::pair<int, int> _message_id_t;
        std::unordered_map<_message_id_t, std::chrono::system_clock::time_point, _message_id_hash> _message_ids;
        
        std::ofstream _server_logs;

        std::mutex _files_index_m;
        std::mutex _modified_files_m;
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
            char request;
            //initialize connection by getting request type
            if (recv(socket_fd, &request, sizeof(request), 0) < 0) {
                log("conn unidentified", "closing connection");
                close(socket_fd);
                return;
            }

            switch (request) {
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

        // handle requests from neighbor peers
        void handle_peer_request(int socket_fd) {
            char request;
            // get request type from node
            if (recv(socket_fd, &request, sizeof(request), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                close(socket_fd);
                return;
            }

            switch (request) {
                case '1':
                    query(socket_fd);
                    break;
                case '2':
                    invalidate(socket_fd);
                    break;
                case '3':
                    compare(socket_fd);
                    break;
                default:
                    log("peer unresponsive", "ignoring request");
                    close(socket_fd);
                    return;
            }
        }

        // query local files index and broadcast message to neighbor peers
        void query(int socket_fd) {
            // get the ttl value of the sent message
            int ttl;
            if (recv(socket_fd, &ttl, sizeof(ttl), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                close(socket_fd);
                return;
            }

            // get the id part of the message id
            int id;
            if (recv(socket_fd, &id, sizeof(id), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                close(socket_fd);
                return;
            }

            // get the sequence message part of the message id
            int sequence_number;
            if (recv(socket_fd, &sequence_number, sizeof(sequence_number), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                close(socket_fd);
                return;
            }            

            char buffer[MAX_FILENAME_SIZE];
            // recieve filename
            if (recv(socket_fd, buffer, sizeof(buffer), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                close(socket_fd);
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
                close(socket_fd);
                return;
            }
            close(socket_fd);
            log("peer disconnected", "closed connection");
        }

        // invalidate cached file and broadcast message to neighbor peers
        void invalidate(int socket_fd) {
            // get the ttl value of the sent message
            int ttl;
            if (recv(socket_fd, &ttl, sizeof(ttl), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                close(socket_fd);
                return;
            }

            // get the id part of the message id
            int id;
            if (recv(socket_fd, &id, sizeof(id), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                close(socket_fd);
                return;
            }

            // get the sequence message part of the message id
            int sequence_number;
            if (recv(socket_fd, &sequence_number, sizeof(sequence_number), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                close(socket_fd);
                return;
            }            

            char buffer[MAX_FILENAME_SIZE];
            // recieve filename
            if (recv(socket_fd, buffer, sizeof(buffer), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                close(socket_fd);
                return;
            }

            // get the version number of the file
            time_t version;
            if (recv(socket_fd, &version, sizeof(version), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                close(socket_fd);
                return;
            }

            // check if message id has been seen/forwarded already
            if (check_message_id(socket_fd)) {
                invalidate_nodes(id, buffer, version);
                if (ttl-- > 0)
                    invalidate_peers(id, buffer, version, sequence_number, ttl);
            }
            close(socket_fd);
            log("peer disconnected", "closed connection");
        }

        void compare(int socket_fd) {
            // get the ttl value of the sent message
            int ttl;
            if (recv(socket_fd, &ttl, sizeof(ttl), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                close(socket_fd);
                return;
            }

            // get the id part of the message id
            int id;
            if (recv(socket_fd, &id, sizeof(id), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                close(socket_fd);
                return;
            }

            // get the sequence message part of the message id
            int sequence_number;
            if (recv(socket_fd, &sequence_number, sizeof(sequence_number), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                close(socket_fd);
                return;
            }            


            char buffer[MAX_FILENAME_SIZE];
            // recieve filename
            if (recv(socket_fd, buffer, sizeof(buffer), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                close(socket_fd);
                return;
            }

            // get the version number of the file
            time_t version;
            if (recv(socket_fd, &version, sizeof(version), 0) < 0) {
                log("peer unresponsive", "ignoring request");
                close(socket_fd);
                return;
            }

            // check if message id has been seen/forwarded already
            if (check_message_id(socket_fd)) {
                // only compare file with current nodes if the file exists in the filex index
                if (_files_index.find(buffer) != _files_index.end())
                    compare_nodes(id, buffer, version);
                if (ttl-- > 0) {
                    compare_peers(buffer, id, sequence_number, ttl, version);
                }
            }
            close(socket_fd);
            log("peer disconnected", "closed connection");
        }

        // compare a modified file from the origin server with cached versions in leaf nodes
        void compare_nodes(int id, std::string filename, time_t version) {
            for (auto&& node : _nodes) {
                // ignore message for origin server
                if (node == id)
                    continue;
                // only send invalidate message if node has registered the file then super peer
                if((std::find(_files_index[filename].begin(), _files_index[filename].end(), node) != _files_index[filename].end())) {
                    int socket_fd = connect_server(node);
                    if (socket_fd < 0) {
                        log("failed node connection", "ignoring connection");
                        continue;
                    }
                    if (send(socket_fd, "0", sizeof(char), 0) < 0)
                        log("node unresponsive", "ignoring request");
                    else {
                        // send the origin node of the file
                        if (send(socket_fd, &id, sizeof(id), 0) < 0)
                            log("node unresponsive", "ignoring request");
                        else {
                            // send the filname to check to invalidate
                            char buffer[MAX_FILENAME_SIZE];
                            strcpy(buffer, filename.c_str());
                            if (send(socket_fd, buffer, sizeof(buffer), 0) < 0)
                                log("node unresponsive", "ignoring request");
                            else {
                                // send the version number of the file
                                if (send(socket_fd, &version, sizeof(version), 0) < 0)
                                    log("node unresponsive", "ignoring request");
                            }
                        }
                    }
                    close(socket_fd);
                }
            } 
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
                    case '6':
                        print_modified_files_list();
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

        void remove_file_from_index(int id, std::string filename) {
            std::lock_guard<std::mutex> guard(_files_index_m);
            // remove peer's id from file
            _files_index[filename].erase(std::remove(_files_index[filename].begin(),
                                            _files_index[filename].end(), id), _files_index[filename].end());
            // remove filename from mapping if no more peers mapped to file
            if (_files_index[filename].size() == 0)
                _files_index.erase(filename);
        }

        // handles communication with node for deregistering a single file 
        void deregistry(int socket_fd, int id) {
            char buffer[MAX_FILENAME_SIZE];
            if (recv(socket_fd, buffer, sizeof(buffer), 0) < 0) {
                remove_node(socket_fd, id, "node unresponsive");
                return;
            }

            time_t version;
            if (recv(socket_fd, &version, sizeof(version), 0) < 0) {
                remove_node(socket_fd, id, "node unresponsive");
                return;
            }
 
            remove_file_from_index(id, buffer);
            if (version != -1) {
                // checks if either consistency method is used to invalidate cached files
                if (_consistency_method == PUSH) {
                    invalidate_nodes(id, buffer, version);
                    invalidate_peers(id, buffer, version, ++_sequence_number, _ttl);
                }
                else if (_consistency_method == PULL_P) {
                    // adds modified files to a temporary list to be dealt with when the TTR expires
                    std::lock_guard<std::mutex> guard(_modified_files_m);
                    _modified_files.push_back({buffer, id, version});
                }
            }
        }

        // sends an invalidation message to all nodes connected nodes
        void invalidate_nodes(int id, std::string filename, time_t version) {
            for (auto&& node : _nodes) {
                // ignore origin node
                if (node == id)
                    continue;
                int socket_fd = connect_server(node);
                if (socket_fd < 0) {
                    log("failed node connection", "ignoring connection");
                    continue;
                }
                if (send(socket_fd, "0", sizeof(char), 0) < 0)
                    log("node unresponsive", "ignoring request");
                else {
                    // send origin node to node
                    if (send(socket_fd, &id, sizeof(id), 0) < 0)
                        log("node unresponsive", "ignoring request");
                    else {
                        // send filename
                        char buffer[MAX_FILENAME_SIZE];
                        strcpy(buffer, filename.c_str());
                        if (send(socket_fd, buffer, sizeof(buffer), 0) < 0)
                            log("node unresponsive", "ignoring request");
                        else {
                            // send version to node
                            if (send(socket_fd, &version, sizeof(version), 0) < 0)
                                log("node unresponsive", "ignoring request");
                        }
                    }
                }
                close(socket_fd);
            }
        }

        // broadcast invalidation message to all neighbor peers
        void invalidate_peers(int id, std::string filename, time_t version, int sequence_number, int ttl) {
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
                    if (send(socket_fd, "2", sizeof(char), 0) < 0)
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
                                        // send the version of the file of the request
                                        time_t version;
                                        if (send(socket_fd, &version, sizeof(version), 0) < 0)
                                            log("peer unresponsive", "ignoring request");
                                        else {
                                            if (!send_message_id(socket_fd, id, sequence_number))
                                                log("peer unresponsive", "ignoring request");
                                            else {
                                                std::string msg = "msg id [" + std::to_string(id) + "," +
                                                                  std::to_string(sequence_number) +
                                                                  "] to peer " + std::to_string(peer);
                                                log("forwarding message", msg);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                close(socket_fd);
            }
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
                    if (send(socket_fd, "1", sizeof(char), 0) < 0)
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
                }
                close(socket_fd);
            }
            return ids;
        }

        // broadcast comparison message to all neighbor peers
        void compare_peers(std::string filename, int id, int sequence_number, int ttl, time_t version) {
            for (auto&& peer : _peers) {
                int socket_fd = connect_server(peer);
                if (socket_fd < 0) {
                    log("failed peer connection", "ignoring connection");
                    continue;
                }
                if (send(socket_fd, "0", sizeof(char), 0) < 0)
                    log("peer unresponsive", "ignoring request");
                else {
                    if (send(socket_fd, "3", sizeof(char), 0) < 0)
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
                                        // send version of the file of the request
                                        if (send(socket_fd, &version, sizeof(version), 0) < 0)
                                            log("peer unresponsive", "ignoring request");
                                        else {
                                            if (!send_message_id(socket_fd, id, sequence_number))
                                                log("peer unresponsive", "ignoring request");
                                            else {
                                                std::string msg = "msg id [" + std::to_string(id) +
                                                                "," + std::to_string(sequence_number) +
                                                                "] to peer " + std::to_string(peer);
                                                log("forwarding message", msg);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                close(socket_fd);
            }
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

        // helper function for displaying all tracked modified files
        void print_modified_files_list() {
            std::cout << "\n__________MODIFIED FILES__________" << std::endl;
            std::cout << "[filename] [origin node] [version]" << std::endl;
            for (auto &&x : _modified_files)
                std::cout << '[' << x.name << "] [" << x.id << "] [" << x.version << ']' << std::endl;
            std::cout << "__________________________________\n" << std::endl;
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

            config >> _consistency_method;
            if (_consistency_method == PULL_N || _consistency_method == PULL_P) {
                config >> _ttr;
            }

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

        // thread for comparing any modified files to local leaf nodes and neighbor super peers
        void check_peers() {
            while (1) {
                sleep(_ttr);
                std::lock_guard<std::mutex> guard(_modified_files_m);
                for (auto&& x: _modified_files) {
                    // only compare files with local nodes if the file exists in the mapping
                    if (_files_index.find(x.name) != _files_index.end())
                        compare_nodes(x.id, x.name, x.version);
                    compare_peers(x.name, x.id, ++_sequence_number, _ttl, x.version);
                }
                // clear the modified files after any cached versions have been invalidated across the network
                _modified_files.clear();
            }
        }

    public:
        int _ttl;
        int _id;
        int _port;
        int _socket_fd;
        int _consistency_method;
        int _ttr;
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
            std::thread m_t(&SuperPeer::maintain_message_ids, this);
            m_t.detach();

            // start thread for maintaining modified files list if using the PULL FROM PEERS consistency method
            if (_consistency_method == PULL_P) {
                std::thread f_t(&SuperPeer::check_peers, this);
                f_t.detach();
            }


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
