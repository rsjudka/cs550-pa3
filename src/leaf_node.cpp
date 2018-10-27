#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/sendfile.h>

#include <thread>
#include <mutex>
#include <iostream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <chrono>
#include <fstream>


#define HOST "localhost" // assume all connections happen on same machine
#define MAX_FILENAME_SIZE 256 // assume the maximum file size is 256 characters
#define MAX_MSG_SIZE 4096
#define MAX_STAT_MSG_SIZE 16


enum CONSISTENCY_METHODS{PUSH, PULL};

//global counters used only for logging special messages used for later anlaysis
int SRCH_REQ_COUNTER = 0;
int OBTN_REQ_COUNTER = 0;


class LeafNode {
    private:
        std::vector<std::pair<std::string, time_t>> _local_files; // vector of all files within a node's directory
        
        struct _remote_file {
            std::string local_name;
            std::string origin_name;
            int origin_node;
            time_t version;
            int ttr;
            std::chrono::time_point<std::chrono::system_clock> check_time;
            bool valid;
        };
        std::vector<_remote_file> _remote_files;
        std::ofstream _server_log;
        std::ofstream _client_log;

        std::mutex _log_m;
        std::mutex _remote_files_m;

        // helper function for getting the current time to microsecond-accuracy as a string
        std::string time_now() {
            std::chrono::high_resolution_clock::duration now = std::chrono::high_resolution_clock::now().time_since_epoch();
            std::chrono::microseconds now_ms = std::chrono::duration_cast<std::chrono::microseconds>(now);
            return std::to_string(now_ms.count());
        }

        // log message to specified log file
        void log(std::ofstream &log_stream, std::string type, std::string msg) {
            std::lock_guard<std::mutex> guard(_log_m);
            log_stream << '[' << time_now() << "] [" << type << "] " << msg  << '\n' << std::endl;
        }

        //special log messages used for later analysis
        void eval_log(std::ofstream &log_stream, int key, std::string type, std::string msg) {
            std::lock_guard<std::mutex> guard(_log_m);
            log_stream << '!' << key << " [" << time_now() << "] [" << type << "] [" << msg  << "]\n" << std::endl;
        }
        
        void error(std::string type) {
            std::cerr << "\n[" << type << "] exiting program\n" << std::endl;
            exit(1);
        }

        // handle all requests sent to the peer
        void handle_connection(int socket_fd) {
            char request;
            //initialize connection by getting id
            if (recv(socket_fd, &request, sizeof(request), 0) < 0) {
                log(_server_log, "conn unidentified", "closing connection");
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
                    log(_server_log, "conn unidentified", "closing connection");
                    close(socket_fd);
                    return;
            }
        }

        void handle_peer_request(int socket_fd) {
            int id;
            if (recv(socket_fd, &id, sizeof(id), 0) < 0)
                log(_server_log, "peer unresponsive", "ignoring request");
            else {
                char buffer[MAX_FILENAME_SIZE];
                if (recv(socket_fd, buffer, sizeof(buffer), 0) < 0)
                    log(_server_log, "peer unresponsive", "ignoring request");
                else {
                    time_t version;
                    if (recv(socket_fd, &version, sizeof(version), 0) < 0)
                        log(_server_log, "peer unresponsive", "ignoring request");
                    else {                              
                        auto it = std::find_if(_remote_files.begin(), _remote_files.end(), [id, buffer, version](const _remote_file
                                                &e){ return e.origin_node == id && e.origin_name == buffer && e.version != version; });
                        if(it != _remote_files.end()) {
                            it->valid = false;
                            std::string filename_path = _remote_files_path + std::string(it->local_name);
                            remove(filename_path.c_str());
                        }
                    }
                }
            }
            close(socket_fd);
        }

        void handle_node_request(int socket_fd) {
            char request;
            // get request type from node
            if (recv(socket_fd, &request, sizeof(request), 0) < 0) {
                log(_server_log, "client unresponsive", "closing connection");
                close(socket_fd);
                return;
            }

            switch (request) {
                case '1':
                    handle_obtain_request(socket_fd);
                    break;
                case '2':
                    handle_poll_request(socket_fd);
                    break;
                default:
                    log(_server_log, "client unresponsive", "closing connection");
                    close(socket_fd);
                    return;
            }
        }

        // handles a node server's file retrieval request
        // only performs single retrieval
        void handle_obtain_request(int socket_fd) {
            // recieve filename to download from node client
            char buffer[MAX_FILENAME_SIZE];
            if (recv(socket_fd, buffer, MAX_FILENAME_SIZE, 0) < 0) {
                // TODO: cleanup
                log(_server_log, "client unresponsive", "closing connection");
                close(socket_fd);
                return;
            }

            // create full file path of node server to send
            std::ostringstream filename;
            filename << std::string(_local_files_path);
            filename << std::string(buffer);

            // TODO: first check if file in local or remote folder
            //       assume to search first in local??

            int fd = open(filename.str().c_str(), O_RDONLY);
            if (fd == -1) {
                // send message to node client if file cannot be opened
                if (send(socket_fd, "-1", MAX_STAT_MSG_SIZE, 0) < 0)
                    log(_server_log, "client unresponsive", "closing connection");
            }
            else {
                struct stat file_stat;
                if (fstat(fd, &file_stat) < 0) {
                    // send message to node client if file size cannot be determined
                    if (send(socket_fd, "-2", MAX_STAT_MSG_SIZE, 0) < 0)
                        log(_server_log, "client unresponsive", "closing connection");
                }
                else {
                    char file_size[MAX_STAT_MSG_SIZE];
                    sprintf(file_size, "%ld", file_stat.st_size);

                    //send file size to node client
                    if (send(socket_fd, file_size, sizeof(file_size), 0) < 0) {
                        // TODO: cleanup
                        log(_server_log, "client unresponsive", "closing connection");
                        close(fd);
                        close(socket_fd);
                        return;
                    }

                    time_t version = -1;
                    auto it = std::find_if(_local_files.begin(), _local_files.end(), [buffer](const std::pair<std::string, time_t>
                                                                                            &e){ return e.first == buffer; });
                    if(it != _local_files.end())
                        version = it->second;
                    if (send(socket_fd, &version, sizeof(version), 0) < 0) {
                        // TODO: cleanup
                        log(_server_log, "client unresponsive", "closing connection");
                        close(fd);
                        close(socket_fd);
                        return;
                    }

                    if (send(socket_fd, &_ttr, sizeof(_ttr), 0) < 0) {
                        // TODO: cleanup
                        log(_server_log, "client unresponsive", "closing connection");
                        close(fd);
                        close(socket_fd);
                        return;
                    }

                    off_t offset = 0;
                    int remaining_size = file_stat.st_size;
                    int sent_size = 0;
                    //send file in 4096 byte blocks until entire file sent
                    while (((sent_size = sendfile(socket_fd, fd, &offset, MAX_MSG_SIZE)) > 0) && (remaining_size > 0))
                        remaining_size -= sent_size;
                }
            }
            close(fd);
            close(socket_fd);
            log(_server_log, "client disconnected", "closed connection");
        }

        void handle_poll_request(int socket_fd) {
            char buffer[MAX_FILENAME_SIZE];
            if (recv(socket_fd, buffer, sizeof(buffer), 0) < 0)
                log(_server_log, "node unresponsive", "ignoring request");
            else {
                time_t version;
                if (recv(socket_fd, &version, sizeof(version), 0) < 0)
                    log(_server_log, "node unresponsive", "ignoring request");
                else {
                    std::pair<std::string, time_t> file_info = {buffer, version};
                    bool valid = std::find(_local_files.begin(), _local_files.end(), file_info) != _local_files.end();
                    if (send(socket_fd, &valid, sizeof(valid), 0) < 0) {
                        log(_server_log, "node unresponsive", "ignoring request"); 
                    }
                }
            }
            close(socket_fd);
        }

        // read all files in node's directory and save to files vector
        std::vector<std::pair<std::string, time_t>> get_files() {
            std::vector<std::pair<std::string, time_t>> tmp_files;
            
            if (auto directory = opendir(_local_files_path.c_str())) {
                while (auto file = readdir(directory)) {
                    //skip . and .. files and any directories
                    if (!file->d_name || strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0 || file->d_type == DT_DIR)
                        continue;
                    
                    // get full file path
                    std::ostringstream file_path;
                    file_path << _local_files_path;
                    file_path << file->d_name;
                    
                    int fd = open(file_path.str().c_str(), O_RDONLY);
                    if (fd == -1) {
                        //ignore file if unable to open
                        log(_client_log, "failed file open", "ignoring \"" + file_path.str() + '\"');
                        continue;
                    }
                    
                    struct stat file_stat;
                    if (fstat(fd, &file_stat) < 0) {
                        // ignore file if unable to file stats
                        log(_client_log, "failed file stat", "ignoring \"" + file_path.str() + '\"');
                        continue;
                    }
                    close(fd);

                    // save a pair of the filename and last modified date to the files vector if not already in there
                    time_t modified_time = file_stat.st_mtim.tv_sec;
                    std::pair<std::string, time_t> file_info = std::make_pair(file->d_name, modified_time);
                    if(!(std::find(tmp_files.begin(), tmp_files.end(), file_info) != tmp_files.end()))
                        tmp_files.push_back(file_info);
                }
                closedir(directory);
            }
            else {
                error("invalid directory");
            }
            return tmp_files;
        }

        // create a connection to some server given a specific port
        // peer flag used for knowing which type of server to connect
        int connect_server(int port, bool peer=true) {
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
                // only exit program if failed to connect to peer
                if (peer)
                    error("failed peer connection");
                else {
                    return -1;
                }
            }
            
            return socket_fd;
        }

        void register_files(int socket_fd) {
            int n;
            char buffer[MAX_FILENAME_SIZE];

            while (1) {
                std::vector<std::pair<std::string, time_t>> tmp_files = get_files();
                for(auto&& x: _local_files) {
                    char request = '1';
                    // send a deregister request if a file is no longer in the files vector (or has been modified)
                    if(!(std::find(tmp_files.begin(), tmp_files.end(), x) != tmp_files.end()))
                        request = '2';
                    
                    // send a register request for any other files
                    if (send(socket_fd, &request, sizeof(request), 0) < 0) {
                        log(_client_log, "server unresponsive", "ignoring request");
                    }
                    else {
                        bzero(buffer, MAX_FILENAME_SIZE);
                        strcpy(buffer, x.first.c_str());
                        // register file with the peer
                        if (send(socket_fd, buffer, sizeof(buffer), 0) < 0)
                            log(_client_log, "server unresponsive", "ignoring request");
                        
                        if (request == '2') {
                            if (send(socket_fd, &x.second, sizeof(x.second), 0) < 0)
                                log(_client_log, "server unresponsive", "ignoring request");
                        }
                    }
                }
                // replace the old files vector with the new one
                _local_files = tmp_files;

                auto time_now = std::chrono::system_clock::now();
                for (auto it = _remote_files.begin(); it < _remote_files.end();) {
                    if (_consistency_method == PULL) {
                        if (std::chrono::duration_cast<std::chrono::seconds>(time_now - it->check_time).count() >= it->ttr)
                            poll_origin_node(std::ref(*it));
                    }

                    char request = '1';
                    if (it->valid == false) {
                        request = '2';
                    }
                    if (send(socket_fd, &request, sizeof(request), 0) < 0) {
                        log(_client_log, "server unresponsive", "ignoring request");
                    }
                    else {
                        bzero(buffer, MAX_FILENAME_SIZE);
                        strcpy(buffer, it->origin_name.c_str());
                        // register file with the peer
                        if (send(socket_fd, buffer, sizeof(buffer), 0) < 0)
                            log(_client_log, "server unresponsive", "ignoring request");
                        
                        if (request == '2') {
                            time_t version = -1;
                            if (send(socket_fd, &version, sizeof(version), 0) < 0)
                                log(_client_log, "server unresponsive", "ignoring request");
                            else {
                                std::lock_guard<std::mutex> guard(_remote_files_m);
                                _remote_files.erase(it);
                            }
                        }
                        else
                            it++;
                    }
                }
                // wait 5 seconds to update files list 
                sleep(5);
            }
        }

        void poll_origin_node(_remote_file &remote_file) {
            remote_file.check_time = std::chrono::system_clock::now();
            int socket_fd = connect_server(remote_file.origin_node);
            if (socket_fd < 0) {
                log(_client_log, "failed node connection", "ignoring connection");
                return;
            }
            if (send(socket_fd, "1", sizeof(char), 0) < 0)
                log(_client_log, "node unresponsive", "ignoring request");
            else {
                if (send(socket_fd, "2", sizeof(char), 0) < 0)
                    log(_client_log, "node unresponsive", "ignoring request");
                else {
                    char buffer[MAX_FILENAME_SIZE];
                    strcpy(buffer, remote_file.origin_name.c_str());
                    if (send(socket_fd, buffer, sizeof(buffer), 0) < 0)
                        log(_client_log, "peer unresponsive", "ignoring request");
                    else {
                        if (send(socket_fd, &remote_file.version, sizeof(remote_file.version), 0) < 0)
                            log(_client_log, "peer unresponsive", "ignoring request");
                        else {
                            if (recv(socket_fd, &remote_file.valid, sizeof(remote_file.valid), 0) < 0)
                               log(_client_log, "peer unresponsive", "ignoring request");
                            else {
                                std::cout << "version " << remote_file.valid << std::endl;
                                if (!remote_file.valid) {
                                    std::string filename_path = _remote_files_path + remote_file.local_name;
                                    remove(filename_path.c_str());
                                }
                            }
                        }
                    }
                }
            }
            close(socket_fd);
        }
        
        // handle user interface for sending a search request to the peer
        void search_request(int socket_fd) {
            std::cout << "filename: ";
            char filename[MAX_FILENAME_SIZE];
            std::cin >> filename;
            eval_log(_client_log, SRCH_REQ_COUNTER, "search request", "start");
            // send a search request to the peer
            if (send(socket_fd, "3", sizeof(char), 0) < 0) {
                std::cout << "\nunexpected connection issue: no search performed\n" << std::endl;
                log(_client_log, "server unresponsive", "ignoring request");
            }
            else {
                // send the filename to search to the peer
                if (send(socket_fd, filename, sizeof(filename), 0) < 0) {
                    std::cout << "\nunexpected connection issue: no search performed\n" << std::endl;
                    log(_client_log, "server unresponsive", "ignoring request");
                }
                else {
                    // recieve list of nodes with file from peer
                    // output appropriate message to node client
                    char buffer[MAX_MSG_SIZE];
                    if (recv(socket_fd, buffer, sizeof(buffer), 0) < 0) {
                        std::cout << "\nunexpected connection issue: no search performed\n" << std::endl;
                        log(_client_log, "server unresponsive", "ignoring request");
                    }
                    else if (!buffer[0])
                        std::cout << "\nfile \"" << filename << "\" not found\n" << std::endl;
                    else
                        std::cout << "\nnode(s) with file \"" << filename << "\": " << buffer << '\n' << std::endl;
                }
            }
            eval_log(_client_log, SRCH_REQ_COUNTER++, "search request", "end");
        }

        //helper function for creating the filename of a downloaded file
        std::string resolve_filename(std::string filename, std::string(node)) {
            std::ostringstream local_filename;
            local_filename << _remote_files_path;
            size_t extension_idx = filename.find_last_of('.');
            local_filename << filename.substr(0, extension_idx);
            // add the file origin if the file already exists in the local "remote" directory
            if ((std::find_if(_remote_files.begin(), _remote_files.end(), [filename, node](const _remote_file
                                                        &e){ return e.local_name == filename && e.origin_node != std::stoi(node); }) != _remote_files.end()))
                local_filename << "-origin-" << node;
            local_filename << filename.substr(extension_idx, filename.size() - extension_idx);

            return local_filename.str();
        }
        
        // handle user interface for sending a retrieve request to a node server
        void obtain_request() {
            std::cout << "node: ";
            char node[6];
            std::cin >> node;
            eval_log(_client_log, OBTN_REQ_COUNTER, "retrieve request", "start");
            // check if the passed-in node is the current client
            if (atoi(node) == _port) {
                std::cout << "\nnode '" << node << "' is current client: no retreival performed\n" << std::endl;
                return;
            }
            // connect to the given node server
            int socket_fd = connect_server(atoi(node), false);
            if (socket_fd < 0) {
                std::cout << "\nnode '" << node << "' is not valid: no retreival performed\n" << std::endl;
                log(_client_log, "failed node server connection", "ignoring request");
                return;
            }
            
            if (send(socket_fd, "1", sizeof(char), 0) < 0) {
                std::cout << "\nunexpected connection issue: no retreival performed\n" << std::endl;
                log(_client_log, "node unresponsive", "ignoring request");
            }
            else {
                if (send(socket_fd, "1", sizeof(char), 0) < 0) {
                    std::cout << "\nunexpected connection issue: no retreival performed\n" << std::endl;
                    log(_client_log, "node unresponsive", "ignoring request");
                }
                else {
                    eval_log(_client_log, OBTN_REQ_COUNTER, "retrieve request", "pause");
                    std::cout << "filename: ";
                    char filename[MAX_FILENAME_SIZE];
                    std::cin >> filename;
                    eval_log(_client_log, OBTN_REQ_COUNTER, "retrieve request", "unpause");
                    if (send(socket_fd, filename, sizeof(filename), 0) < 0) {
                        std::cout << "\nunexpected connection issue: no retreival performed\n" << std::endl;
                        log(_client_log, "node unresponsive", "ignoring request");
                    }
                    else {
                        char buffer[MAX_STAT_MSG_SIZE];
                        // get the file size from the node server
                        if (recv(socket_fd, buffer, sizeof(buffer), 0) < 0) {
                            std::cout << "\nunexpected connection issue: no retreival performed\n" << std::endl;
                            log(_client_log, "node unresponsive", "ignoring request");
                        }
                        else {
                            int file_size = atoi(buffer);
                            // handle message from node server
                            if (file_size == -1)
                                std::cout << "\nnode '" << node << "' does not have file \"" << filename << "\": no retreival performed\n" << std::endl;
                            else if (file_size == -2)
                                std::cout << "\ncould not read file \"" << filename << "\"'s stats: no retreival performed\n" << std::endl;
                            else {
                                time_t version;
                                if (recv(socket_fd, &version, sizeof(version), 0) < 0) {
                                    std::cout << "\nunexpected connection issue: no retreival performed\n" << std::endl;
                                    log(_client_log, "node unresponsive", "ignoring request");
                                }
                                else {
                                    int ttr;
                                    if (recv(socket_fd, &ttr, sizeof(ttr), 0) < 0) {
                                        std::cout << "\nunexpected connection issue: no retreival performed\n" << std::endl;
                                        log(_client_log, "node unresponsive", "ignoring request");
                                    }
                                    else {
                                        // create pretty filename for outputting results to node client
                                        std::string local_filename_path = resolve_filename(filename, node);
                                        size_t filename_idx = local_filename_path.find_last_of('/');
                                        std::string local_filename = local_filename_path.substr(filename_idx+1, local_filename_path.size() - filename_idx);
                                        FILE *file = fopen(local_filename_path.c_str(), "w");
                                        if (file == NULL) {
                                            std::cout << "\nunable to create new file \"" << local_filename << "\": no retreival performed\n" << std::endl;
                                            log(_client_log, "failed file open", "ignoring file");
                                        }
                                        else {
                                            char buffer_[MAX_MSG_SIZE];
                                            int remaining_size = file_size;
                                            int received_size;
                                            // write blocks recieved from node server to new file
                                            while (((received_size = recv(socket_fd, buffer_, sizeof(buffer_), 0)) > 0) && (remaining_size > 0)) {
                                                fwrite(buffer_, sizeof(char), received_size, file);
                                                remaining_size -= received_size;
                                            }
                                            fclose(file);
                                            auto it = std::find_if(_remote_files.begin(), _remote_files.end(), [filename, node](const _remote_file
                                                                        &e){ return e.origin_name == filename && e.origin_node == std::stoi(node); });
                                            if(it == _remote_files.end()) {
                                                _remote_files.push_back({local_filename, filename, std::stoi(node), version, ttr, std::chrono::system_clock::now(), true});
                                                std::cout << "\nfile \"" << filename << "\" downloaded as \"" << local_filename << "\"\n" << std::endl;
                                            }
                                            else {
                                                it->version = version;
                                                std::cout << "\nfile \"" << local_filename << "\" updated to version " << version << "\n" << std::endl;
                                            }
                                            std::cout << "\ndislpay file '" << local_filename << "'\n. . .\n" << std::endl;
                                            log(_client_log, "file download", "file download successful");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            eval_log(_client_log, OBTN_REQ_COUNTER++, "retrieve request", "end");
            close(socket_fd);
        }

        void refresh() {

        }

        void print_files() {
            std::cout << "\n__________LOCAL FILES__________" << std::endl;
            for (auto &&x : _local_files) {
                std::cout << x.first << "..." << x.second << std::endl;
            }
            std::cout << "_______________________________\n" << std::endl;
            std::cout << "\n__________REMOTE FILES__________" << std::endl;
            for (auto &&x : _remote_files) {
                std::cout << x.local_name << "..." << x.origin_name << "..." << x.origin_node << "..." << x.valid << "..." << x.version << std::endl;
            }
            std::cout << "_______________________________\n" << std::endl;
        }

        // gets the static network info for a node
        void get_network(std::string config_path) {
            // open a stream to the config file
            std::ifstream config(config_path);
            int ttl;
            int member_type;
            int id;
            int port;
            int peer_id;

            config >> _consistency_method;
            if (_consistency_method == PULL) {
                config >> _ttr;
            }

            // proper config syntax is expected to be followed
            config >> ttl;
            std::string tmp;
            while(config >> member_type) {
                if (member_type == 1) {
                    config >> id >> port >> peer_id;
                    if (id == _id) {
                        _port = port;
                        _peer_id = peer_id;
                        return;
                    }
                }
                else
                    std::getline(config, tmp); // ignore anything else in the line
            }
            error("invalid id");
        }

    public:
        std::string _local_files_path;
        std::string _remote_files_path;
        int _id;
        int _port;
        int _peer_id;
        int _socket_fd;
        int _consistency_method;
        int _ttr = 0;

        LeafNode(int id, std::string config_path, std::string directory) {
            _id = id;
            get_network(config_path);
            
            // add ending '/' if missing in directory argument
            if (directory.back() != '/')
                directory += '/';
            _local_files_path = directory + "local/";
            _remote_files_path = directory + "remote/";
            _local_files = get_files();

            struct sockaddr_in addr;
            socklen_t addr_size = sizeof(addr);
            bzero((char*)&addr, addr_size);
            
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port = htons(_port);

            _socket_fd = socket(AF_INET, SOCK_STREAM, 0);

            // bind socket to port to be used for node server
            if (bind(_socket_fd, (struct sockaddr*)&addr, addr_size) < 0)
                error("failed to start node server");

            std::cout << "current node id: " << _port << '\n' << std::endl;

            // start logging for both node client and node server
            std::string log_name_prefix = "logs/leaf_nodes/" + std::to_string(_port);
            _server_log.open(log_name_prefix + "_server.log");
            _client_log.open(log_name_prefix + "_client.log");
        }
        
        void run_client() {
            int socket_fd = connect_server(_peer_id);

            //send type id to be used as client id in peer
            if (send(socket_fd, "1", sizeof(char), 0) < 0)
                error("server unreachable");

            //send node server port number to be used as client id in peer
            if (send(socket_fd, &_port, sizeof(_port), 0) < 0)
                error("server unreachable");

            //start thread for automatic files updater
            std::thread t(&LeafNode::register_files, this, socket_fd);
            t.detach();

            //continously prompt user for request
            while (1) {
                std::string request;
                std::cout << "request [(s)earch|(o)btain|(q)uit]: ";
                std::cin >> request;

                switch (request[0]) {
                    case 's':
                    case 'S':
                        search_request(socket_fd);
                        break;
                    case 'o':
                    case 'O':
                        obtain_request();
                        break;
                    case 'q':
                    case 'Q':
                        close(socket_fd);
                        exit(0);
                        break;
                    case 'l':
                    case 'L':
                        // used for testing to see all registered files
                        send(socket_fd, "4", sizeof(char), 0);
                        break;
                    case 'm':
                    case 'M':
                        // used for testing to see current message ids list
                        send(socket_fd, "5", sizeof(char), 0);
                        break;
                    case 'f':
                    case 'F':
                        print_files();
                        break;
                    case 'r':
                    case 'R':
                        refresh();
                        break;
                    default:
                        std::cout << "\nunexpected request\n" << std::endl;
                        break;
                }
            }
        }

        void run_server() {
            struct sockaddr_in addr;
            socklen_t addr_size = sizeof(addr);
            int socket_fd;

            std::ostringstream connection;
            while (1) {
                // listen for any node connections to start file download
                listen(_socket_fd, 5);

                if ((socket_fd = accept(_socket_fd, (struct sockaddr*)&addr, &addr_size)) < 0) {
                    // ignore any failed connections from node clients
                    log(_server_log, "failed client connection", "ignoring connection");
                    continue;
                }

                connection << inet_ntoa(addr.sin_addr) << '@' << ntohs(addr.sin_port);
                log(_server_log, "client connected", connection.str());

                // start thread for performing file download
                std::thread t(&LeafNode::handle_connection, this, socket_fd);
                t.detach(); // detaches thread and allows for next connection to be made without waiting

                connection.str("");
                connection.clear();
            }
        }

        void run() {
            // start independent threads for both client and server
            std::thread c_t(&LeafNode::run_client, this);
            std::thread s_t(&LeafNode::run_server, this);

            c_t.join();
            s_t.join();
        }

        ~LeafNode() {
            close(_socket_fd);
            _server_log.close();
            _client_log.close();
        }
};


int main(int argc, char *argv[]) {
    // require node id, config path, & directory to be passed as arg
    if (argc < 4) {
        std::cerr << "usage: " << argv[0] << " id config_path directory" << std::endl;
        exit(0);
    }

    LeafNode leaf_node(atoi(argv[1]), argv[2], argv[3]);
    leaf_node.run();

    return 0;
}
