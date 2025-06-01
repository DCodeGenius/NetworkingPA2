// #include <stdio.h>
// #include <stdlib.h>
// #include <string.h>
// #include <float.h>
// #include <math.h>   // fabs
#include <cstdio>   // instead of <stdio.h>
#include <cstdlib>  // instead of <stdlib.h>
#include <cstring>  // instead of <string.h>
#include <cfloat>   // instead of <float.h>
#include <cmath>    // instead of <math.h>

//STL
#include <queue>
#include <vector>
#include <functional>  // for std::greater


#define MAX_IP_LEN 16
#define MAX_LINE_LEN 256
#define MAX_CONNECTIONS 10000
#define INITIAL_PACKET_CAPACITY 100000
#define EPSILON 1e-9

typedef struct {
    char src_ip[MAX_IP_LEN];
    int src_port;
    char dst_ip[MAX_IP_LEN];
    int dst_port;
} Connection;

typedef struct {
    long long arrival_time;
    char src_ip[MAX_IP_LEN];
    int src_port;
    char dst_ip[MAX_IP_LEN];
    int dst_port;
    int length;
    double weight;
    int has_weight;
    char original_line[MAX_LINE_LEN];

    // WFQ specific fields
    double virtual_start_time;
    double virtual_finish_time;
    int connection_id;
    int appearance_order;
    char is_on_bus;
} Packet;

typedef struct {
    Connection conn;
    double weight;
    double virtual_finish_time;
    int appearance_order;
    int active;
} ConnectionInfo;


struct CompareByVFT {
    bool operator()(const Packet& a, const Packet& b) const {
        double diff = a.virtual_finish_time - b.virtual_finish_time;

        if (fabs(diff) > EPSILON) return a.virtual_finish_time > b.virtual_finish_time;
        return a.appearance_order > b.appearance_order;
    }
};

struct CompareByVST {
    bool operator()(const Packet& a, const Packet& b) const {
        double diff = a.virtual_start_time - b.virtual_start_time;

        if (fabs(diff) > EPSILON) return a.virtual_start_time > b.virtual_start_time;
        return a.appearance_order > b.appearance_order;
    }
};

// Global state
ConnectionInfo connections[MAX_CONNECTIONS];
int num_connections = 0;
double virtual_time = 0.0;
double next_departure_time = 0; // Represents when the server becomes free next
std::queue<Packet> pending_packets;
std::priority_queue<Packet, std::vector<Packet>, CompareByVFT> ready_queue;
std::priority_queue<Packet, std::vector<Packet>, CompareByVFT> virtual_bus;
std::priority_queue<Packet, std::vector<Packet>, CompareByVST> wait_for_virtual_bus;
double last_virtual_change = 0.0;
double current_time = 0.0;
char is_packet_on_bus = 0;
int packet_on_bus_idx = 0;
int should_remove_from_virtual_bus = 0;
long long next_virtual_end = 0;
double sum_active_weight = 0.0;
int Debug = 0;
int debug_arrival_time_2 = 438091;
int debug_arrival_time_1 = 538091;
int debug_func_use = 0;

// Function prototypes
int find_or_create_connection(const char* src_ip, int src_port, const char* dst_ip, int dst_port, int appearance_order);
void parse_packet(const char* line, Packet* packet, int appearance_order);
void schedule_next_packet();
char* my_strdup(const char* s);
void parse_file();
void add_to_virtual_bus(Packet* packet);
void handle_packet_arrival(Packet* packet);
void remove_from_virtual_bus();




char* my_strdup(const char* s) {
    size_t len = strlen(s) + 1;
    char* copy = (char*)malloc(len);
    if (copy) {
        memcpy(copy, s, len);
    }
    return copy;
}

int compare_packets_by_virtual_finish_time(const void* a, const void* b) {
    const Packet* pa = (const Packet*)a;
    const Packet* pb = (const Packet*)b;
    if (pa->virtual_finish_time < pb->virtual_finish_time) return -1;
    if (pa->virtual_finish_time > pb->virtual_finish_time) return 1;
    return pa->appearance_order - pb->appearance_order;
}

void parse_file() {
    char line[MAX_LINE_LEN];
    int appearance_order = 0;
    while (fgets(line, sizeof(line), stdin)) {
        line[strcspn(line, "\n")] = 0;
        if (strlen(line) == 0) continue;
        Packet packet;
        parse_packet(line, &packet, appearance_order++);
        pending_packets.push(packet);
    }
}

void progress_real_time(long long next_arrival_event_time) {

    current_time = next_arrival_event_time;
    if (next_departure_time < current_time && is_packet_on_bus){ //&& is_packet_on_bus) {
        current_time = next_departure_time;
    }
    if (!virtual_bus.empty() > 0) {
        // qsort(virtual_bus.packets, virtual_bus.count, sizeof(Packet), compare_packets_by_virtual_finish_time);
        double virtual_finish = virtual_bus.top().virtual_finish_time;
        if ((current_time <= debug_arrival_time_1 && current_time >= debug_arrival_time_2) && Debug == 1) {
            printf("reason for call: compute if next event is virtual departure \n\n");
        }
        double real_finish_virtual = last_virtual_change + (virtual_finish - virtual_time) *(sum_active_weight); //sum_Active_weights(); // sum_active_weight;
        if (Debug == 1 && current_time == 1112533.0) {
            printf("real_finish_virtual = %lf \n", real_finish_virtual);
        }
        if (real_finish_virtual <= current_time + EPSILON) {
            current_time = real_finish_virtual;
            should_remove_from_virtual_bus = 1; // will remove later, need to take into account when computing virtual
        }
    }
}

void progress_virtual_time(long long next_arrival_event_time) {
    double weight_sum =  sum_active_weight; //sum_Active_weights(); //sum_active_weight;

    if (weight_sum >EPSILON) {
        if ((current_time <= debug_arrival_time_1 && current_time >= debug_arrival_time_2) && Debug == 1) {
            printf("virtual_time: %f, new virtual time %f, current_time %lf\n", virtual_time, virtual_time + (double)(current_time - last_virtual_change) / weight_sum, current_time);
            printf("weight sum %f, time diff %lf \n", weight_sum, current_time - last_virtual_change);
            printf("next arrival %lld current time %lf, next dep %lf\n", next_arrival_event_time, current_time, next_departure_time);
        }
        virtual_time += (double)(current_time - last_virtual_change) / weight_sum;

    }
    last_virtual_change = current_time;
}

void remove_from_virtual_bus() {
    if ((virtual_bus.top().arrival_time <= debug_arrival_time_1  && virtual_bus.top().arrival_time >=  debug_arrival_time_2) && Debug == 1) {
        printf("removing packet %s virtual time %lf\n", virtual_bus.top().original_line, virtual_time);
    }
    sum_active_weight -= virtual_bus.top().weight;
    virtual_bus.pop();
}

void add_to_virtual_bus(Packet* packet_to_add) {
    sum_active_weight += packet_to_add->weight;
    virtual_bus.push(*packet_to_add);
}

void handle_packet_arrival(Packet* packet) {
    pending_packets.pop();

    int conn_id = packet->connection_id;
    double last_conn_vft = connections[conn_id].virtual_finish_time;

    double virtual_start = (virtual_time > last_conn_vft) ? virtual_time : last_conn_vft;


    if ((packet->arrival_time <= debug_arrival_time_1  && packet->arrival_time >=  debug_arrival_time_2) && Debug == 1) {
        printf("DEBUG: %s virtual start: %f lastconfft %f virtual time %f \n", packet->original_line, virtual_start, last_conn_vft, virtual_time);
    }


    packet->virtual_start_time = virtual_start;
    if (packet->has_weight) {
        connections[conn_id].weight = packet->weight;
    }else{packet->weight = connections[conn_id].weight;} //if packet does not have a specified weight, take the connection's at the time
    packet->virtual_finish_time = virtual_start + (double)packet->length / connections[conn_id].weight;

    if ((packet->arrival_time <= debug_arrival_time_1  && packet->arrival_time >=  debug_arrival_time_2) && Debug == 1) {
        printf("DEBUG: %s Virtual End %f length %d weight %lf \n", packet->original_line, packet->virtual_finish_time, packet->length, connections[packet->connection_id].weight);
    }
    connections[conn_id].virtual_finish_time = packet->virtual_finish_time;

    ready_queue.push(*packet);
}
int main() {
    parse_file();
    while (!pending_packets.empty() || !ready_queue.empty()) {
        if (Debug == 1) {
            printf("ready_queue.size() = %lld \n", ready_queue.size());
        }
        long long next_arrival_event_time = (!pending_packets.empty()) ? pending_packets.front().arrival_time : LLONG_MAX;
        if (ready_queue.empty() && next_arrival_event_time == LLONG_MAX) {
            break;
        }

        // Virtual and Real time progression
        progress_real_time(next_arrival_event_time);
        progress_virtual_time(next_arrival_event_time);

        if (current_time == DBL_MAX) {
            break;
        }


    // }

    if (should_remove_from_virtual_bus) {
        remove_from_virtual_bus();
        should_remove_from_virtual_bus = 0;
    }

     //move packet from wait for virtual to virtual if needed
    if (!wait_for_virtual_bus.empty() && wait_for_virtual_bus.top().virtual_start_time <= virtual_time + EPSILON) {
        Packet packet_to_move = wait_for_virtual_bus.top();
        add_to_virtual_bus(&packet_to_move);
        wait_for_virtual_bus.pop();
    }

    if (current_time >= next_departure_time && is_packet_on_bus ==  1) {
        is_packet_on_bus = 0;
    }
    // Process all packets that have arrived by this current_time
    while (!pending_packets.empty() && pending_packets.front().arrival_time <= current_time) {
        Packet packet = pending_packets.front();
        // Find or create connection
        packet.connection_id = find_or_create_connection(packet.src_ip, packet.src_port,
                                                         packet.dst_ip, packet.dst_port,
                                                         packet.appearance_order);
        handle_packet_arrival(&packet);

        if (packet.virtual_start_time != virtual_time) {
            wait_for_virtual_bus.push(packet);
        }else {
            add_to_virtual_bus(&packet);
        }
    }

    // schedule a packet if it is time to do so
    if (!ready_queue.empty() && next_departure_time <= current_time && is_packet_on_bus == 0){
        schedule_next_packet();
    }



    }

    return 0;
}


int find_or_create_connection(const char* src_ip, int src_port, const char* dst_ip, int dst_port, int appearance_order) {
    // Look for existing connection
    for (int i = 0; i < num_connections; i++) {
        if (strcmp(connections[i].conn.src_ip, src_ip) == 0 &&
            connections[i].conn.src_port == src_port &&
            strcmp(connections[i].conn.dst_ip, dst_ip) == 0 &&
            connections[i].conn.dst_port == dst_port) {
            return i;
        }
    }

    // Create new connection
    if (num_connections >= MAX_CONNECTIONS) {
        fprintf(stderr, "Too many connections\n");
        exit(1);
    }

    int id = num_connections++;
    strcpy(connections[id].conn.src_ip, src_ip);
    connections[id].conn.src_port = src_port;
    strcpy(connections[id].conn.dst_ip, dst_ip);
    connections[id].conn.dst_port = dst_port;
    connections[id].weight = 1; // Default weight - THIS MUST BE 1
    connections[id].virtual_finish_time = 0.0;
    connections[id].appearance_order = appearance_order;
    connections[id].active = 0;

    return id;
}

void parse_packet(const char* line, Packet* packet, int appearance_order) {
    strcpy(packet->original_line, line);
    packet->appearance_order = appearance_order;
    packet->has_weight = 0;
    packet -> is_on_bus = 0;

    char* line_copy = my_strdup(line);
    char* token = strtok(line_copy, " ");
    int field = 0;

    while (token != NULL) {
        switch (field) {
            case 0: packet->arrival_time = atoll(token); break;
            case 1: strcpy(packet->src_ip, token); break;
            case 2: packet->src_port = atoi(token); break;
            case 3: strcpy(packet->dst_ip, token); break;
            case 4: packet->dst_port = atoi(token); break;
            case 5: packet->length = atoi(token); break;
            case 6:
                packet->weight = atof(token);
                packet->has_weight = 1;
                break;
        }
        field++;
        token = strtok(NULL, " ");
    }

    free(line_copy);



}



void schedule_next_packet() {
    if (ready_queue.empty()) return;


        Packet packet_to_send = ready_queue.top();
        packet_to_send.is_on_bus = 1;
    //remove_packet_from_queue(&ready_queue, best_idx);
        is_packet_on_bus = 1;
        //packet_on_bus_idx = best_idx;
        ready_queue.pop();
    if ((current_time <= debug_arrival_time_1 && current_time >= debug_arrival_time_2) && Debug == 1) {
        printf("new packet on bus at time %lf, %s\n", current_time, packet_to_send.original_line);
    }


    // Determine actual start time for this packet
    // real_time currently holds when the server *became free* from the *previous* transmission (or 0 if idle)
    long long actual_start_time = (next_departure_time > packet_to_send.arrival_time) ? next_departure_time : packet_to_send.arrival_time;

    // Original output format restored
    printf("%lld: %s\n", actual_start_time, packet_to_send.original_line);



    // Update virtual time
    // Calculate weight sum of connections that were in ready_queue *before* this packet was removed
    // double current_weight_sum = 0;

    // Update server's next free time
    next_departure_time = actual_start_time + packet_to_send.length;
}

