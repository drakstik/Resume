//lang::CwC

// Some of the code in this file, particularly the select() loops were interpreted from Beej's Guide to Socking Programming

#pragma once

#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <assert.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <thread>
#include <unistd.h>
#include <mutex>
#include <vector>

#include "map.h"
#include "deserial.h"

#define PORT "8080"
// The fixed number of nodes that this network supports (1 server, the rest are clients)
#define BACKLOG 6
// The size of the string buffer used to send messages
#define BUF_SIZE 10000

/**
 * This class represents a key/value store maintained on one node from a larger distributed system.
 * It also holds all of the functionality needed to exchange data with the other nodes over a 
 * network.
 * 
 * @author Spencer LaChance <lachance.s@northeastern.edu>
 * @author David Mberingabo <mberingabo.d@husky.neu.edu>
 */
class KVStore : public Object {
public:
    // Index of this KVStore's node
    size_t idx_;
    // Number of nodes in the system
    size_t num_nodes_;
    // The map from string keys to deserialized data blobs
    Map map_;
    // Have we received an Ack?
    bool ack_recvd_;
    // Data returned in a Reply message after a Get message is sent
    const char* reply_data_;
    // Data returned in a Reply message after a WaitAndGet message is sent
    // WaitAndGet gets its own variable so that there is no confusion between threads running both
    // get operations
    const char* wag_reply_data_;
    // The thread that runs the select() loop
    std::thread* t_;
    // Vector of threads that process messages
    std::vector<std::thread>* threads_;
    // The lock that prevents data races
    std::mutex mtx_;
    // has this node shut down?
    bool has_shutdown;

    /**
     * Constructor that initializes an empty KVStore.
     * 
     * @param idx   The index of the node running this KVStore.
     * @param nodes The total number of nodes running in the system.
     */
    KVStore(size_t idx, size_t nodes) : idx_(idx), num_nodes_(nodes), ack_recvd_(false),
        reply_data_(nullptr), wag_reply_data_(nullptr) {
        threads_ = new std::vector<std::thread>();
        startup_();
        // Wait a second for client registration to finish
        sleep(1);
    }

    /**
     * Destructor
     */
    ~KVStore() {
        t_->join();
        for (std::thread& th : *threads_) {
            if (th.joinable()) {
                th.join();
            }
        }
        delete t_;
        delete threads_;
    }

    /**
     * Serializes the given data and puts it into the map at the given key.
     * 
     * @param k The key at which the data will be stored
     * @param v The serialized data that will be stored in the k/v store
     */
    void put(Key& k, const char* v) {
        size_t dst_node = k.get_home_node();
        // Check if the key corresponds to this node
        if (dst_node == idx_) {
            // If so, put the data in this KVStore's map
            mtx_.lock();
            map_.put(*k.get_keystring(), new String(v));
            mtx_.unlock();
        } else {
            // If not, send a Put message to the correct node
            Put p(&k, v);
            const char* msg = p.serialize();
            send_to_node_(msg, dst_node);
            // Wait for an Ack confirming that the data was stored successfully
            while (!ack_recvd_) {
                sleep(1);
                if (has_shutdown) exit(-1);
            }
            ack_recvd_ = false;
            delete[] msg;
        }
        delete[] v;
    }

    /**
     * Gets the data stored at the given key, deserializes it, and returns it.
     * 
     * @param k The key at which the reqested data is stored
     * 
     * @return The serialized data blob
     */
    const char* get(Key& k) {
        size_t dst_node = k.get_home_node();
        const char* res;
        // Check if this key corresponds to this node
        if (dst_node == idx_) {
            // If so, get the data from this KVStore's map
            mtx_.lock();
            String* serialized_data = dynamic_cast<String*>(map_.get(*k.get_keystring()));
            assert(serialized_data != nullptr);
            mtx_.unlock();
            // Copy the data because the Map owns it
            String* copy = serialized_data->clone();
            res = copy->steal();
            delete copy;
        } else {
            // If not, send a Get message to the correct node
            Get g(&k);
            const char* msg = g.serialize();
            send_to_node_(msg, dst_node);
            // Wait for a reply with the desired data
            while (reply_data_ == nullptr) {
                sleep(1);
                if (has_shutdown) exit(-1);
            }
            res = reply_data_;
            reply_data_ = nullptr;
            delete[] msg;
        }
        return res;
    }

    /**
     * Waits until there is data in the store at the given key, and then gets it, deserializes it,
     *  and returns it.
     * 
     * @param k The key at which the reqested data is stored
     * 
     * @return The serialized data blob
     */
    const char* wait_and_get(Key& k) {
        size_t dst_node = k.get_home_node();
        // Check if this key corresponds to this node
        if (dst_node == idx_) {
            // If so, wait until the data is put into this node's map
            String* key_string = k.get_keystring();
            bool contains_key = map_.contains(*key_string);
            while (!contains_key) {
                sleep(1);
                contains_key = map_.contains(*key_string);
                if (has_shutdown) exit(-1);
            }
            // Get the data
            return get(k);
        } else {
            // If not, send a WaitAndGet message to the correct node
            WaitAndGet wag(&k);
            const char* msg = wag.serialize();
            send_to_node_(msg, dst_node);
            // Wait for a reply with the desired data
            while (wag_reply_data_ == nullptr) {
                sleep(1);
                if (has_shutdown) exit(-1);
            }
            const char* res = wag_reply_data_;
            wag_reply_data_ = nullptr;
            delete[] msg;
            return res;
        }
    }

    /** Retuns the number of nodes running in the system. */
    size_t num_nodes() { return num_nodes_; }

    /** Returns the current node's index. */
    size_t this_node() { return idx_; }

    // ############################# NETWORK-SPECIFIC FIELDS AND METHODS ###########################

    char* ip_;
    // This node's socket file descriptor
    int fd_;
    // struct that will be filled with basic info in order to generate
    // other full structs used to build sockets
    struct addrinfo hints_;
    // The file descriptor and address info of another node connecting to this once
    int their_fd_;
    struct sockaddr_storage their_addr_;
    // A string buffer used to send messages
    char* buffer_;
    // An array of socket file descriptors to the other nodes
    // The array indices are the node indices of each node
    int* nodes_;
    // master file descriptor list
    fd_set master_;
    // temporary fd list used by select()
    fd_set read_fds_;
    // the maxmimum fd value in the master list
    int fdmax_;

    // The server's directory containing every client IP
    // Used by the server only
    Directory* directory_;
    
    // The file descriptor for the socket connected to the Server
    // Used by the client only
    int servfd_;

    /**
     * Is this node running the role of the server?
     */
    bool is_server() { return idx_ == 0; }

    /**
     * Startup protocol.
     * Creates the socket that clients will connect to this node through.
     * If this is a client, it also creates a socket to the server and sends it a message containing
     * the client's IP and node index.
     * 
     * @param idx The index of the current node.
     */
    void startup_() {
        buffer_ = new char[BUF_SIZE];
        ip_ = idx_to_ip_(idx_);
        has_shutdown = false;
        // This is an array that maps the indices of each node to their socket fds
        nodes_ = new int[BACKLOG];
        for (int i = 0; i < BACKLOG; i++) nodes_[i] = -1;

        // Fill an addrinfo struct for this node, configuring its options, address, and port
        struct addrinfo *info;
        memset(&hints_, 0, sizeof(hints_));
        hints_.ai_family = AF_INET;
        hints_.ai_socktype = SOCK_STREAM;
        exit_if_not(getaddrinfo(ip_, PORT, &hints_, &info) == 0, "Call to getaddrinfo() failed");
        // Use the struct to create a socket
        exit_if_not((fd_ = socket(info->ai_family, info->ai_socktype, info->ai_protocol)) >= 0,
            "Call to socket() failed");
        // Bind the IP and port to the socket
        exit_if_not(bind(fd_, info->ai_addr, info->ai_addrlen) >= 0, "Call to bind() failed");
        freeaddrinfo(info);
        if (is_server()) {
            directory_ = new Directory();
        } else {
            // Wait a bit for the server to start up
            usleep(250000);
            // Calculate the server's IP using its node index (always 0) and use that to generate 
            // another struct
            struct addrinfo *servinfo;
            char* serv_ip = idx_to_ip_(0);
            exit_if_not(getaddrinfo(serv_ip, PORT, &hints_, &servinfo) == 0,
                "Call to getaddrinfo() failed");
            // Create the socket to the Server
            exit_if_not((servfd_ = socket(servinfo->ai_family, servinfo->ai_socktype, 
                servinfo->ai_protocol)) >= 0, "Call to socket() failed");
            // Connect to the Server
            int cnct = connect(servfd_, servinfo->ai_addr, servinfo->ai_addrlen);
            while (cnct < 0) {
                // Wait indefinitely until the lead node is available
                p("Node ", idx_).p(idx_, idx_).pln(": Connection to lead node failed.", idx_);
                sleep(1);
                cnct = connect(servfd_, servinfo->ai_addr, servinfo->ai_addrlen);
                if (has_shutdown) exit(-1);
            }
            p("Node ", idx_).p(idx_, idx_).pln(": Connection to lead node succeeded.", idx_);
            // Add the server fd to the fd/idx map
            nodes_[0] = servfd_;
            freeaddrinfo(servinfo);
            // Send IP to server in a Register message
            Register reg(new String(ip_), idx_);
            const char* msg = reg.serialize();
            exit_if_not(send(servfd_, msg, strlen(msg) + 1, 0) > 0, "Sending IP to server failed");
            delete[] msg; delete[] serv_ip;
        }
        // Start listening for incoming messages
        t_ = new std::thread(&KVStore::monitor_sockets_, this);
    }

    /**
     * Shutdown protocol.
     * Closes all sockets and deletes all fields.
     */
    void shutdown() {
        has_shutdown = true;
        if (is_server()) {
            delete directory_;
        } else {
            close(servfd_);
        }
        clear_map_();
        close(fd_);
        delete[] ip_;
        delete[] buffer_;
        delete[] nodes_;
    }

    /**
     * Send a message to a specific node.
     * 
     * @param msg The message to be sent
     * @param dst The index of the destination node
     */
    void send_to_node_(const char* msg, size_t dst) {
        exit_if_not(dst < num_nodes_, "Invalid dst node index");
        int fd = nodes_[dst];
        while (fd == -1) {
            // Wait indefinitely until the desired node is available
            sleep(1);
            p("Node ", idx_).p(idx_, idx_).p(": Could not find a node with index ", idx_)
                .pln(dst, idx_);
            fd = nodes_[dst];
            if (has_shutdown) exit(-1);
        }
        exit_if_not(send(fd, msg, strlen(msg) + 1, 0) > 0, "Sending msg to other client failed");
    }

    /**
     * Listens for incoming messages coming from other nodes on the network and then processes them
     * accordingly.
     */
    void monitor_sockets_() {
        // length of other node's addrinfo struct
        socklen_t addrlen;
        // number of bytes read by recv()
        int nbytes;
        // The number of nodes on the network
        int num_nodes = 1;
        // The timeout argument for select()
        struct timeval tv;
        tv.tv_sec = 3;
        tv.tv_usec = 0;
        // Used to concat messages bigger than BUFSIZE
        StrBuff buff;
        // Clear the two fd lists
        FD_ZERO(&master_);
        FD_ZERO(&read_fds_);
        // Start listening
        exit_if_not(listen(fd_, BACKLOG) == 0, "Call to listen() failed");
        // Add this Client's fd and the Server's to the master list and use them to initialize
        // the max fd value
        FD_SET(fd_, &master_);
        if (is_server()) {
            fdmax_ = fd_;
        } else {
            FD_SET(servfd_, &master_);
            fdmax_ = fd_ > servfd_ ? fd_ : servfd_;
        }
        // Main loop
        for (;;) {
            read_fds_ = master_; // Copy the master list
            // Select the existing socket connections and iterate through them
            if (select(fdmax_ + 1, &read_fds_, NULL, NULL, &tv) < 0) return;
            for (int i = 0; i <= fdmax_; i++) {
                // In case shutdown() was called from the other thread
                if (has_shutdown) return;
                if (FD_ISSET(i, &read_fds_)) {
                    if (i == fd_) {
                        // Found a new connection
                        if (num_nodes + 1 > BACKLOG) {
                            // The network is full, do not accept this connection
                            p("Node ", idx_).p(idx_, idx_)
                                .pln(": Network is full, cannot accept new connection.", idx_);
                            continue;
                        }
                        num_nodes++;
                        // The node is receiving a new connection from another one
                        addrlen = sizeof(their_addr_);
                        // Accept the connection and add the new socket fd to the master list
                        exit_if_not((their_fd_ = accept(fd_, (struct sockaddr*)&their_addr_, &addrlen)) >= 0, 
                            "Call to accept() failed");
                        FD_SET(their_fd_, &master_);
                        // Update the max fd value
                        if (their_fd_ > fdmax_) {
                            fdmax_ = their_fd_;
                        }
                    } else {
                        // Receiving a message
                        if ((nbytes = recv(i, buffer_, BUF_SIZE, 0)) <= 0) {
                            // Connection to the other node was closed or there was an error,
                            // so shut down
                            shutdown();
                            return;
                        } else {
                            buff.c(buffer_, nbytes);
                            // Serialized messages are terminated with newlines, so if the buffer
                            // does not end with a newline, the message was chunked
                            if (buffer_[nbytes - 2] != '\n') continue;
                            // Figure out what kind of message was received
                            char* serial_msg = buff.c_str();
                            Deserializer ds(serial_msg);
                            Message* m = ds.deserialize_message();
                            assert(m != nullptr);
                            switch (m->kind()) {
                                case MsgKind::Directory: process_directory_(m->as_directory()); break;
                                case MsgKind::Register: process_register_(m->as_register(), i); break;
                                case MsgKind::Reply: process_reply_(m->as_reply()); break;
                                case MsgKind::Ack: {
                                    // Set the value that put() is waiting for above
                                    ack_recvd_ = true;
                                    delete m;
                                    break;
                                }
                                case MsgKind::Put:
                                    threads_->push_back(std::thread(&KVStore::process_put_, this, m->as_put(), i));
                                    break;
                                case MsgKind::Get:
                                    threads_->push_back(std::thread(&KVStore::process_get_, this, m->as_get(), i));
                                    break;
                                case MsgKind::WaitAndGet:
                                    threads_->push_back(std::thread(&KVStore::process_wag_, this, m->as_wait_and_get(), i));
                                    break;
                                default: shutdown();
                            }
                            delete[] serial_msg;
                        }
                    }
                }
            }
        }
    }

    /**
     * Client function
     * Parse the directory message sent from the server.
     * 
     * @param directory The directory
     */
    void process_directory_(Directory* directory) {
        Vector* ips = directory->get_addresses();
        IntVector* indices = directory->get_indices();
        for (int i = 0; i < ips->size(); i++) {
            char* ip = ips->get(i)->c_str();
            if (strcmp(ip, ip_) != 0) connect_to_client_(ip, indices->get(i));
        }
        delete directory;
    }

    /**
     * Process the given Register message sent by another node wanting to connect with this one.
     */
    void process_register_(Register* reg, int fd) {
        char* new_ip = reg->get_ip()->c_str();
        size_t new_idx = reg->get_sender();
        if (is_server()) {
            // A client is registering with the server
            // Add the new IP to the directory
            directory_->add_client(new_ip, new_idx);
            // Send the updated directory back to the client
            const char* serial_directory = directory_->serialize();
            exit_if_not(send(fd, serial_directory, strlen(serial_directory) + 1, 0) > 0,
                "Call to send() failed");
            delete[] serial_directory;
        }
        // Keep track of the sender's socket fd and node index
        nodes_[new_idx] = fd;
        delete reg;
    }

    void process_reply_(Reply* rep) {
        MsgKind req = rep->get_request();
        const char* v = rep->get_value();
        // Set the values that get() and wait_and_get() wait for above
        if (req == MsgKind::WaitAndGet)
            wag_reply_data_ = v;
        else
            reply_data_ = v;
        delete rep;
    }

    /**
     * Processes the given Put message.
     * 
     * @param p  The message
     * @param fd The socket fd to send the Ack back to
     */
    void process_put_(Put* p, int fd) {
        Key* k = p->get_key();
        const char* v = p->get_value();
        // Ensure that this message was sent to the right node
        exit_if_not(k->get_home_node() == idx_, "Put was sent to incorrect node");
        put(*k, v);

        // Reply with an Ack confirming that the put operation was successful
        Ack* a = new Ack();
        const char* msg = a->serialize();
        exit_if_not(send(fd, msg, strlen(msg) + 1, 0) > 0, "Call to send() failed");
        delete p; delete k; delete a; delete[] msg;
    }

    /**
     * Starts the get operation in a separate thread
     */
    void process_get_(Get* g, int fd) {
        Key* k = g->get_key();
        // Ensure that this message was sent to the right node
        exit_if_not(k->get_home_node() == idx_, "Put was sent to incorrect node");
        const char* res = get(*k);

        // Send back a Reply with the data
        Reply r(res, MsgKind::Get);
        const char* msg = r.serialize();
        exit_if_not(send(fd, msg, strlen(msg) + 1, 0) > 0, "Call to send() failed");
        delete g; delete k; delete[] msg; delete[] res;
    }

    /**
     * Starts the wait_and_get operation in a separate thread
     */
    void process_wag_(WaitAndGet* wag, int fd) {
        Key* k = wag->get_key();
        // Ensure that this message was sent to the right node
        exit_if_not(k->get_home_node() == idx_, "Put was sent to incorrect node");
        const char* res = wait_and_get(*k);

        // Send back a Reply with the data
        Reply r(res, MsgKind::WaitAndGet);
        const char* msg = r.serialize();
        exit_if_not(send(fd, msg, strlen(msg) + 1, 0) > 0, "Call to send() failed");
        delete wag; delete k; delete[] msg; delete[] res;
    }

    /**
     * Client function
     * Create a socket to the client at the given IP, connect to it, and send it a Register message.
     * 
     * @param ip  The IP address of the other client
     * @param idx The node index of the other client
     */
    void connect_to_client_(char* ip, size_t idx) {
        struct addrinfo* client_info;
        int client_fd;
        // Generate an addrinfo struct for the other client
        memset(&hints_, 0, sizeof(hints_));
        hints_.ai_family = AF_INET;
        hints_.ai_socktype = SOCK_STREAM;
        exit_if_not(getaddrinfo(ip, PORT, &hints_, &client_info) == 0, 
            "Call to getaddrinfo() failed");
        // Create a socket to connect to the client
        exit_if_not((client_fd = socket(client_info->ai_family, client_info->ai_socktype, 
            client_info->ai_protocol)) >= 0, "Call to socket() failed");
        // Connect to the client
        exit_if_not(connect(client_fd, client_info->ai_addr, client_info->ai_addrlen) >= 0, 
            "Call to connect() failed");
        freeaddrinfo(client_info);
        // Send the client a Register message
        Register reg(new String(ip_), idx_);
        const char* msg = reg.serialize();
        exit_if_not(send(client_fd, msg, strlen(msg) + 1, 0) > 0, 
            "Sending Register to other client failed");
        // Add the fd to the master list
        FD_SET(client_fd, &master_);
        // Update the max fd value
        if (client_fd > fdmax_) {
            fdmax_ = client_fd;
        }
        // Keep track of the client's fd and node index
        nodes_[idx] = client_fd;
        delete[] msg;
    }

    /**
     * Empties the fd/ip map and closes every fd
     */
    void clear_map_() {
        for (int i = 0; i < BACKLOG; i++) {
            int fd = nodes_[i];
            if (fd != -1) {
                close(fd);
                nodes_[i] = -1;
            }
        }
    }

    /**
     * The IP address of a node is 127.0.0.x where x is the node index + 1.
     * So given a node index, this function returns the corresponding IP address.
     */
    char* idx_to_ip_(size_t idx) {
        char* ip = new char[INET_ADDRSTRLEN + 1];
        int bytes = sprintf(ip, "127.0.0.%zu", idx + 1);
        exit_if_not(bytes <= INET_ADDRSTRLEN + 1, "Invalid index");
        return ip;
    }
};
