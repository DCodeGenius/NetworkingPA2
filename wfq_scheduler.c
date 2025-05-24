#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <float.h>

#define MAX_IP_LEN 16
#define MAX_LINE_LEN 256
#define MAX_CONNECTIONS 10000
#define INITIAL_PACKET_CAPACITY 100000

typedef struct {
    char src_ip[MAX_IP_LEN];
    int src_port;
    char dst_ip[MAX_IP_LEN];
    int dst_port;
} Connection;

typedef struct {
    double arrival_time;
    char src_ip[MAX_IP_LEN];
    int src_port;
    char dst_ip[MAX_IP_LEN];
    int dst_port;
    int length;
    int weight;
    int has_weight;
    char original_line[MAX_LINE_LEN];
    
    // WFQ specific fields
    double virtual_start_time;
    double virtual_finish_time;
    int connection_id;
    int appearance_order;
} Packet;

typedef struct {
    Connection conn;
    int weight;
    double virtual_finish_time;
    int appearance_order;
    int active;
} ConnectionInfo;

typedef struct {
    Packet *packets;
    int count;
    int capacity;
} PacketQueue;

// Global state
ConnectionInfo connections[MAX_CONNECTIONS];
int num_connections = 0;
double virtual_time = 0.0;
double real_time = 0.0;
PacketQueue pending_packets = {NULL, 0, 0};
PacketQueue ready_queue = {NULL, 0, 0};

// Function prototypes
int find_or_create_connection(const char* src_ip, int src_port, const char* dst_ip, int dst_port, int appearance_order);
void parse_packet(const char* line, Packet* packet, int appearance_order);
void add_packet_to_queue(PacketQueue* queue, const Packet* packet);
void remove_packet_from_queue(PacketQueue* queue, int index);
int compare_packets_by_arrival_time(const void* a, const void* b);
void update_virtual_time();
void process_packets_up_to_time(double current_time);
void schedule_next_packet();
void init_packet_queue(PacketQueue* queue);
void cleanup();
char* my_strdup(const char* s);

// Helper function to duplicate string (portable strdup)
char* my_strdup(const char* s) {
    size_t len = strlen(s) + 1;
    char* copy = malloc(len);
    if (copy) {
        memcpy(copy, s, len);
    }
    return copy;
}

// Comparison function for qsort
int compare_packets_by_arrival_time(const void* a, const void* b) {
    const Packet* pa = (const Packet*)a;
    const Packet* pb = (const Packet*)b;
    if (pa->arrival_time < pb->arrival_time) return -1;
    if (pa->arrival_time > pb->arrival_time) return 1;
    return pa->appearance_order - pb->appearance_order;
}

int main() {
    char line[MAX_LINE_LEN];
    int appearance_order = 0;
    
    init_packet_queue(&pending_packets);
    init_packet_queue(&ready_queue);
    
    // Read all packets first
    while (fgets(line, sizeof(line), stdin)) {
        // Remove newline
        line[strcspn(line, "\n")] = 0;
        if (strlen(line) == 0) continue;
        
        Packet packet;
        parse_packet(line, &packet, appearance_order++);
        add_packet_to_queue(&pending_packets, &packet);
    }
    
    // Sort pending packets by arrival time
    qsort(pending_packets.packets, pending_packets.count, sizeof(Packet), compare_packets_by_arrival_time);
    
    // Process packets in chronological order
    while (pending_packets.count > 0 || ready_queue.count > 0) {
        // Find next event time
        double next_arrival = (pending_packets.count > 0) ? pending_packets.packets[0].arrival_time : DBL_MAX;
        double next_finish = (ready_queue.count > 0) ? real_time : DBL_MAX;
        
        if (ready_queue.count > 0) {
            // Find the packet with earliest virtual finish time that's ready
            int best_idx = -1;
            double best_virtual_finish = DBL_MAX;
            
            for (int i = 0; i < ready_queue.count; i++) {
                if (ready_queue.packets[i].virtual_finish_time < best_virtual_finish ||
                    (ready_queue.packets[i].virtual_finish_time == best_virtual_finish && 
                     (best_idx == -1 || ready_queue.packets[i].appearance_order < ready_queue.packets[best_idx].appearance_order))) {
                    best_virtual_finish = ready_queue.packets[i].virtual_finish_time;
                    best_idx = i;
                }
            }
            
            if (best_idx >= 0) {
                next_finish = real_time;
            }
        }
        
        // Choose next event
        if (next_arrival <= next_finish) {
            // Process arriving packet
            process_packets_up_to_time(next_arrival);
        } else if (ready_queue.count > 0) {
            // Schedule next packet transmission
            schedule_next_packet();
        } else {
            break;
        }
    }
    
    cleanup();
    return 0;
}

void init_packet_queue(PacketQueue* queue) {
    queue->capacity = INITIAL_PACKET_CAPACITY;
    queue->packets = malloc(queue->capacity * sizeof(Packet));
    queue->count = 0;
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
    connections[id].weight = 1; // Default weight
    connections[id].virtual_finish_time = 0.0;
    connections[id].appearance_order = appearance_order;
    connections[id].active = 0;
    
    return id;
}

void parse_packet(const char* line, Packet* packet, int appearance_order) {
    strcpy(packet->original_line, line);
    packet->appearance_order = appearance_order;
    packet->has_weight = 0;
    
    char* line_copy = my_strdup(line);
    char* token = strtok(line_copy, " ");
    int field = 0;
    
    while (token != NULL) {
        switch (field) {
            case 0: packet->arrival_time = atof(token); break;
            case 1: strcpy(packet->src_ip, token); break;
            case 2: packet->src_port = atoi(token); break;
            case 3: strcpy(packet->dst_ip, token); break;
            case 4: packet->dst_port = atoi(token); break;
            case 5: packet->length = atoi(token); break;
            case 6: 
                packet->weight = atoi(token);
                packet->has_weight = 1;
                break;
        }
        field++;
        token = strtok(NULL, " ");
    }
    
    free(line_copy);
    
    // Find or create connection
    packet->connection_id = find_or_create_connection(packet->src_ip, packet->src_port, 
                                                     packet->dst_ip, packet->dst_port, 
                                                     appearance_order);
    
    // Update connection weight if specified
    if (packet->has_weight) {
        connections[packet->connection_id].weight = packet->weight;
    } else {
        packet->weight = connections[packet->connection_id].weight;
    }
}

void add_packet_to_queue(PacketQueue* queue, const Packet* packet) {
    if (queue->count >= queue->capacity) {
        queue->capacity *= 2;
        queue->packets = realloc(queue->packets, queue->capacity * sizeof(Packet));
    }
    queue->packets[queue->count++] = *packet;
}

void remove_packet_from_queue(PacketQueue* queue, int index) {
    for (int i = index; i < queue->count - 1; i++) {
        queue->packets[i] = queue->packets[i + 1];
    }
    queue->count--;
}

void update_virtual_time() {
    if (ready_queue.count == 0) return;
    
    // Calculate sum of weights of active connections
    double weight_sum = 0.0;
    int active_connections[MAX_CONNECTIONS];
    int num_active = 0;
    
    for (int i = 0; i < ready_queue.count; i++) {
        int conn_id = ready_queue.packets[i].connection_id;
        int found = 0;
        for (int j = 0; j < num_active; j++) {
            if (active_connections[j] == conn_id) {
                found = 1;
                break;
            }
        }
        if (!found) {
            active_connections[num_active++] = conn_id;
            weight_sum += connections[conn_id].weight;
        }
    }
    
    // Virtual time doesn't change if no active connections
    if (weight_sum == 0.0) return;
}

void process_packets_up_to_time(double current_time) {
    real_time = current_time;
    
    // Move all packets that have arrived to ready queue
    while (pending_packets.count > 0 && pending_packets.packets[0].arrival_time <= current_time) {
        Packet packet = pending_packets.packets[0];
        remove_packet_from_queue(&pending_packets, 0);
        
        // Calculate virtual start time and finish time
        int conn_id = packet.connection_id;
        double virtual_start = (virtual_time > connections[conn_id].virtual_finish_time) ? 
                              virtual_time : connections[conn_id].virtual_finish_time;
        
        packet.virtual_start_time = virtual_start;
        packet.virtual_finish_time = virtual_start + (double)packet.length / connections[conn_id].weight;
        
        // Update connection's virtual finish time
        connections[conn_id].virtual_finish_time = packet.virtual_finish_time;
        
        add_packet_to_queue(&ready_queue, &packet);
    }
    
    // Update virtual time to match real time progression
    if (ready_queue.count > 0) {
        update_virtual_time();
    }
}

void schedule_next_packet() {
    if (ready_queue.count == 0) return;
    
    // Find packet with smallest virtual finish time (with tie-breaking)
    int best_idx = 0;
    for (int i = 1; i < ready_queue.count; i++) {
        if (ready_queue.packets[i].virtual_finish_time < ready_queue.packets[best_idx].virtual_finish_time ||
            (ready_queue.packets[i].virtual_finish_time == ready_queue.packets[best_idx].virtual_finish_time &&
             ready_queue.packets[i].appearance_order < ready_queue.packets[best_idx].appearance_order)) {
            best_idx = i;
        }
    }
    
    Packet packet = ready_queue.packets[best_idx];
    remove_packet_from_queue(&ready_queue, best_idx);
    
    // Output the scheduled packet
    printf("%.6f: %s\n", real_time, packet.original_line);
    
    // Advance real time by transmission time
    real_time += packet.length;
    
    // Update virtual time
    if (ready_queue.count > 0) {
        // Calculate weight sum of remaining active connections
        double weight_sum = 0.0;
        int active_connections[MAX_CONNECTIONS];
        int num_active = 0;
        
        for (int i = 0; i < ready_queue.count; i++) {
            int conn_id = ready_queue.packets[i].connection_id;
            int found = 0;
            for (int j = 0; j < num_active; j++) {
                if (active_connections[j] == conn_id) {
                    found = 1;
                    break;
                }
            }
            if (!found) {
                active_connections[num_active++] = conn_id;
                weight_sum += connections[conn_id].weight;
            }
        }
        
        if (weight_sum > 0) {
            virtual_time += packet.length / weight_sum;
        }
    } else {
        virtual_time = real_time;
    }
}

void cleanup() {
    if (pending_packets.packets) free(pending_packets.packets);
    if (ready_queue.packets) free(ready_queue.packets);
} 